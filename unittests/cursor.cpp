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
        BFC_ASSERT_EQUAL(true, bt_cursor_is_nil((ham_bt_cursor_t *)m_cursor));
        BFC_ASSERT_EQUAL(true, bt_cursor_is_nil((ham_bt_cursor_t *)clone));
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
        BFC_REGISTER_TEST(TempTxnCursorTest, closeCoupledBtreeCursorTest);
        BFC_REGISTER_TEST(TempTxnCursorTest, closeUncoupledBtreeCursorTest);
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

        BFC_ASSERT_EQUAL(false, bt_cursor_is_nil((ham_bt_cursor_t *)clone));
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

        BFC_ASSERT_EQUAL(false, bt_cursor_is_nil((ham_bt_cursor_t *)clone));
        ham_key_t *k1=bt_cursor_get_uncoupled_key((ham_bt_cursor_t *)m_cursor);
        ham_key_t *k2=bt_cursor_get_uncoupled_key((ham_bt_cursor_t *)clone);
        BFC_ASSERT_EQUAL(0, strcmp((char *)k1->data, (char *)k2->data));
        BFC_ASSERT_EQUAL(k1->size, k2->size);
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));
    }

    void closeCoupledBtreeCursorTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, 
                    bt_cursor_uncouple((ham_bt_cursor_t *)m_cursor, 0));

        /* will close in teardown() */
    }

    void closeUncoupledBtreeCursorTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* will close in teardown() */
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
        BFC_REGISTER_TEST(NoTxnCursorTest, insertFindMultipleCursorsTest);
        BFC_REGISTER_TEST(NoTxnCursorTest, findInEmptyDatabaseTest);
        BFC_REGISTER_TEST(NoTxnCursorTest, nilCursorTest);
        BFC_REGISTER_TEST(NoTxnCursorTest, moveFirstInEmptyDatabaseTest);
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

    void moveFirstInEmptyDatabaseTest(void)
    {
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
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
        BFC_REGISTER_TEST(LongTxnCursorTest, closeCoupledTxnCursorTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveFirstInEmptyTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveFirstInEmptyTransactionExtendedKeyTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveFirstInTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveFirstInTransactionExtendedKeyTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveFirstIdenticalTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveFirstSmallerInTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveFirstSmallerInTransactionExtendedKeyTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveFirstSmallerInBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveFirstSmallerInBtreeExtendedKeyTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveFirstErasedInTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveFirstErasedInTxnExtendedKeyTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveFirstErasedInsertedInTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest,
                    moveFirstSmallerInBtreeErasedInTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveLastInEmptyTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveLastInEmptyTransactionExtendedKeyTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveLastInTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveLastInTransactionExtendedKeyTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveLastIdenticalTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveLastSmallerInTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveLastSmallerInTransactionExtendedKeyTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveLastSmallerInBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveLastSmallerInBtreeExtendedKeyTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveLastErasedInTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveLastErasedInTxnExtendedKeyTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveLastErasedInsertedInTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest,
                    moveLastSmallerInBtreeErasedInTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, nilCursorTest);

        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextInEmptyTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextInEmptyBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextSmallerInTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextSmallerInBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextSmallerInTransactionSequenceTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextSmallerInBtreeSequenceTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextOverErasedItemTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextOverIdenticalItemsTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveBtreeThenNextOverIdenticalItemsTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveTxnThenNextOverIdenticalItemsTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextOverIdenticalItemsThenBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextOverIdenticalItemsThenTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextOverSequencesOfIdenticalItemsTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextWhileInsertingBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextWhileInsertingTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextWhileInsertingMixedTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveNextWhileErasingTest);

        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousInEmptyTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousInEmptyBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousSmallerInTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousSmallerInBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousSmallerInTransactionSequenceTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousSmallerInBtreeSequenceTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousOverErasedItemTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousOverIdenticalItemsTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveBtreeThenPreviousOverIdenticalItemsTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    moveTxnThenPreviousOverIdenticalItemsTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousOverIdenticalItemsThenBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousOverIdenticalItemsThenTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousOverSequencesOfIdenticalItemsTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousWhileInsertingBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousWhileInsertingTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    movePreviousWhileInsertingMixedTest);

        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    switchDirectionsInBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    switchDirectionsInTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    switchDirectionsMixedStartInBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    switchDirectionsMixedStartInTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    switchDirectionsMixedSequenceTest);

        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findTxnThenMoveNextTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findTxnThenMoveNext2Test);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findTxnThenMoveNext3Test);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findTxnThenMoveNext4Test);

        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findTxnThenMovePreviousTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findTxnThenMovePrevious2Test);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findTxnThenMovePrevious3Test);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findTxnThenMovePrevious4Test);

        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findBtreeThenMoveNextTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findBtreeThenMoveNext2Test);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findBtreeThenMoveNext3Test);

        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findBtreeThenMovePreviousTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    findBtreeThenMovePrevious2Test);

        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    insertThenMoveNextTest);

        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    abortWhileCursorActiveTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    commitWhileCursorActiveTest);

        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    eraseKeyWithTwoCursorsTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    eraseKeyWithTwoCursorsOverwriteTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    eraseWithThreeCursorsTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    eraseKeyWithoutCursorsTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, 
                    eraseKeyAndFlushTransactionsTest);
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
        BFC_ASSERT_EQUAL(true, bt_cursor_is_nil((ham_bt_cursor_t *)m_cursor));

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

        BFC_ASSERT_EQUAL(false, bt_cursor_is_nil((ham_bt_cursor_t *)clone));
        BFC_ASSERT_EQUAL(2u, txn_get_cursor_refcount(m_txn));
        BFC_ASSERT_EQUAL(
                txn_cursor_get_coupled_op(cursor_get_txn_cursor(m_cursor)), 
                txn_cursor_get_coupled_op(cursor_get_txn_cursor(clone)));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));
        BFC_ASSERT_EQUAL(1u, txn_get_cursor_refcount(m_txn));
                
    }

    void closeCoupledTxnCursorTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        
        /* will be closed in teardown() */
                
    }

    void moveFirstInEmptyTransactionTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));
    }

    void moveFirstInEmptyTransactionExtendedKeyTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        const char *ext="123456789012345678901234567890";
        key.data=(void *)ext;
        key.size=31;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp(ext, (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));
    }

    void moveFirstInTransactionTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));
    }

    void moveFirstInTransactionExtendedKeyTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        const char *ext="123456789012345678901234567890";
        key.data=(void *)ext;
        key.size=31;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp(ext, (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));
    }

    void moveFirstIdenticalTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* insert the same key into the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, HAM_OVERWRITE));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));

        /* make sure that the cursor is coupled to the txn-op */
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
    }

    void moveFirstSmallerInTransactionTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a large key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"22222";
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* insert a smaller key into the Transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"xyzab";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xyzab", (char *)rec2.data));
    }

    void moveFirstSmallerInTransactionExtendedKeyTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        const char *ext1="111111111111111111111111111111";
        const char *ext2="222222222222222222222222222222";
        key.size=31;
        rec.size=6;

        /* insert a large key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)ext2;
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* insert a smaller key into the Transaction */
        key.data=(void *)ext1;
        rec.data=(void *)"xyzab";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp(ext1, (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xyzab", (char *)rec2.data));
    }

    void moveFirstSmallerInBtreeTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a small key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* insert a greater key into the Transaction */
        key.data=(void *)"22222";
        rec.data=(void *)"xyzab";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));
    }

    void moveFirstSmallerInBtreeExtendedKeyTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        const char *ext1="111111111111111111111111111111";
        const char *ext2="222222222222222222222222222222";
        key.size=31;
        rec.size=6;

        /* insert a small key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)ext1;
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* insert a greater key into the Transaction */
        key.data=(void *)ext2;
        rec.data=(void *)"xyzab";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp(ext1, (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));
    }

    void moveFirstErasedInTxnTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* erase it */
        key.data=(void *)"11111";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        /* this moves the cursor to the first item, but it was erased
         * and therefore this fails */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    }

    void moveFirstErasedInTxnExtendedKeyTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        const char *ext1="111111111111111111111111111111";
        key.size=31;
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)ext1;
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* erase it */
        key.data=(void *)ext1;
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        /* this moves the cursor to the first item, but it was erased
         * and therefore this fails */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));

        /* we have to manually clear the changeset, otherwise ham_close will
         * fail. The changeset was filled in be->_fun_insert, but this is an
         * internal function which will not clear it. All other functions fail
         * and therefore do not touch the changeset. */
        changeset_clear(env_get_changeset(m_env));
    }

    void moveFirstErasedInsertedInTxnTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* erase it */
        key.data=(void *)"11111";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        /* re-insert it */
        rec.data=(void *)"10101";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("10101", (char *)rec2.data));
    }

    void moveFirstSmallerInBtreeErasedInTxnTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a small key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* insert a greater key into the Transaction */
        key.data=(void *)"22222";
        rec.data=(void *)"xyzab";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* erase the smaller item */
        key.data=(void *)"11111";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        /* this moves the cursor to the second item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xyzab", (char *)rec2.data));
    }

    void moveLastInEmptyTransactionTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));
    }

    void moveLastInEmptyTransactionExtendedKeyTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        const char *ext="123456789012345678901234567890";
        key.data=(void *)ext;
        key.size=31;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp(ext, (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));
    }

    void moveLastInTransactionTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));
    }

    void moveLastInTransactionExtendedKeyTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        const char *ext="123456789012345678901234567890";
        key.data=(void *)ext;
        key.size=31;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp(ext, (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));
    }

    void moveLastIdenticalTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* insert the same key into the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, HAM_OVERWRITE));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));

        /* make sure that the cursor is coupled to the txn-op */
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
    }

    void moveLastSmallerInTransactionTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a large key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"22222";
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* insert a smaller key into the Transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"xyzab";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));
    }

    void moveLastSmallerInTransactionExtendedKeyTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        const char *ext1="111111111111111111111111111111";
        const char *ext2="222222222222222222222222222222";
        key.size=31;
        rec.size=6;

        /* insert a large key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)ext2;
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* insert a smaller key into the Transaction */
        key.data=(void *)ext1;
        rec.data=(void *)"xyzab";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp(ext2, (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec2.data));
    }

    void moveLastSmallerInBtreeTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a small key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* insert a greater key into the Transaction */
        key.data=(void *)"22222";
        rec.data=(void *)"xyzab";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xyzab", (char *)rec2.data));
    }

    void moveLastSmallerInBtreeExtendedKeyTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        const char *ext1="111111111111111111111111111111";
        const char *ext2="222222222222222222222222222222";
        key.size=31;
        rec.size=6;

        /* insert a small key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)ext1;
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* insert a greater key into the Transaction */
        key.data=(void *)ext2;
        rec.data=(void *)"xyzab";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp(ext2, (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xyzab", (char *)rec2.data));
    }

    void moveLastErasedInTxnTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* erase it */
        key.data=(void *)"11111";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        /* this moves the cursor to the last item, but it was erased
         * and therefore this fails */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    }

    void moveLastErasedInTxnExtendedKeyTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        const char *ext1="111111111111111111111111111111";
        key.size=31;
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)ext1;
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* erase it */
        key.data=(void *)ext1;
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        /* this moves the cursor to the last item, but it was erased
         * and therefore this fails */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));

        /* we have to manually clear the changeset, otherwise ham_close will
         * fail. The changeset was filled in be->_fun_insert, but this is an
         * internal function which will not clear it. All other functions fail
         * and therefore do not touch the changeset. */
        changeset_clear(env_get_changeset(m_env));
    }

    void moveLastErasedInsertedInTxnTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* erase it */
        key.data=(void *)"11111";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        /* re-insert it */
        rec.data=(void *)"10101";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("10101", (char *)rec2.data));
    }

    void moveLastSmallerInBtreeErasedInTxnTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a small key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"abcde";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* insert a greater key into the Transaction */
        key.data=(void *)"22222";
        rec.data=(void *)"xyzab";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* erase the smaller item */
        key.data=(void *)"11111";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        /* this moves the cursor to the second item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xyzab", (char *)rec2.data));
    }

    void moveNextInEmptyTransactionTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a few keys into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    }

    void moveNextInEmptyBtreeTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a few keys into the btree */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    }

    void moveNextSmallerInTransactionTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a "small" key into the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        /* and a "large" one in the btree */
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    }

    void moveNextSmallerInBtreeTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a "small" key into the btree */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* and a "large" one in the txn */
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    }

    void moveNextSmallerInTransactionSequenceTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a few "small" keys into the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        ham_backend_t *be=db_get_backend(m_db);
        /* and a few "large" keys in the btree */
        key.data=(void *)"44444";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"55555";
        rec.data=(void *)"eeeee";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"66666";
        rec.data=(void *)"fffff";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("44444", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("55555", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("eeeee", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("66666", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("fffff", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    }

    void moveNextSmallerInBtreeSequenceTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a few "small" keys into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* and a few "large" keys in the transaction */
        key.data=(void *)"44444";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        key.data=(void *)"55555";
        rec.data=(void *)"eeeee";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        key.data=(void *)"66666";
        rec.data=(void *)"fffff";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("44444", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("55555", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("eeeee", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("66666", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("fffff", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    }

    void moveNextOverErasedItemTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a few "small" keys into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* erase the one in the middle */
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_erase(m_db, m_txn, &key, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    }

    void moveNextOverIdenticalItemsTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a few keys into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* overwrite the same keys in the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"22222";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"33333";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    }

    void moveBtreeThenNextOverIdenticalItemsTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        ham_backend_t *be=db_get_backend(m_db);
        /* insert a few keys into the btree */
        key.data=(void *)"00000";
        rec.data=(void *)"xxxxx";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* skip the first key, and overwrite all others in the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"22222";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"33333";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT(!(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN));
        BFC_ASSERT_EQUAL(0, strcmp("00000", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xxxxx", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    }

    void moveTxnThenNextOverIdenticalItemsTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        key.data=(void *)"00000";
        rec.data=(void *)"xxxxx";
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, m_txn, &key, &rec, 0));
        ham_backend_t *be=db_get_backend(m_db);
        /* insert a few keys into the btree */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* skip the first key, and overwrite all others in the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"22222";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"33333";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("00000", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xxxxx", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    }

    void moveNextOverIdenticalItemsThenBtreeTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        ham_backend_t *be=db_get_backend(m_db);
        /* insert a few keys into the btree */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"99999";
        rec.data=(void *)"xxxxx";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* skip the first key, and overwrite all others in the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"22222";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"33333";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(!(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN));
        BFC_ASSERT_EQUAL(0, strcmp("99999", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xxxxx", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    }

    void moveNextOverIdenticalItemsThenTxnTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        ham_backend_t *be=db_get_backend(m_db);
        /* insert a few keys into the btree */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"99999";
        rec.data=(void *)"xxxxx";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, 0));
        /* skip the first key, and overwrite all others in the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"22222";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"33333";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("99999", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xxxxx", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    }

    ham_status_t insertBtree(const char *key, const char *rec, 
                        ham_u32_t flags=0)
    {
        ham_key_t k={0};
        k.data=(void *)key;
        k.size=strlen(key)+1;
        ham_record_t r={0};
        r.data=(void *)rec;
        r.size=strlen(rec)+1;

        ham_backend_t *be=db_get_backend(m_db);
        return (be->_fun_insert(be, &k, &r, flags));
    }

    ham_status_t insertTxn(const char *key, const char *rec, 
                        ham_u32_t flags=0)
    {
        ham_key_t k={0};
        k.data=(void *)key;
        k.size=strlen(key)+1;
        ham_record_t r={0};
        r.data=(void *)rec;
        r.size=strlen(rec)+1;

        return (ham_insert(m_db, m_txn, &k, &r, flags));
    }

    ham_status_t eraseTxn(const char *key)
    {
        ham_key_t k={0};
        k.data=(void *)key;
        k.size=strlen(key)+1;

        return (ham_erase(m_db, m_txn, &k, 0));
    }

#define BTREE 1
#define TXN   2
    ham_status_t compare(const char *key, const char *rec, int where)
    {
        ham_key_t k={0};
        ham_record_t r={0};
        ham_status_t st;

        st=ham_cursor_move(m_cursor, &k, &r, HAM_CURSOR_NEXT);
        if (st)
            return (st);
        if (strcmp(key, (char *)k.data))
            return (HAM_INTERNAL_ERROR);
        if (strcmp(rec, (char *)r.data))
            return (HAM_INTERNAL_ERROR);
        if (where==BTREE) {
            if (cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN)
                return (HAM_INTERNAL_ERROR);
        }
        else {
            if (!(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN))
                return (HAM_INTERNAL_ERROR);
        }
        return (0);
    }

    ham_status_t comparePrev(const char *key, const char *rec, int where)
    {
        ham_key_t k={0};
        ham_record_t r={0};
        ham_status_t st;

        st=ham_cursor_move(m_cursor, &k, &r, HAM_CURSOR_PREVIOUS);
        if (st)
            return (st);
        if (strcmp(key, (char *)k.data))
            return (HAM_INTERNAL_ERROR);
        if (strcmp(rec, (char *)r.data))
            return (HAM_INTERNAL_ERROR);
        if (where==BTREE) {
            if (cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN)
                return (HAM_INTERNAL_ERROR);
        }
        else {
            if (!(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN))
                return (HAM_INTERNAL_ERROR);
        }
        return (0);
    }

    void moveNextOverSequencesOfIdenticalItemsTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11113", "aaaaa", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11114", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11115", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("11116", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11117", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11118", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11116", "bbbba", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11117", "bbbbb", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11118", "bbbbc", HAM_OVERWRITE));

        BFC_ASSERT_EQUAL(0, compare    ("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11115", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11116", "bbbba", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11117", "bbbbb", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11118", "bbbbc", TXN));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, 0));
    }

    void moveNextWhileInsertingBtreeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("11116", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11117", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11118", "aaaac"));

        BFC_ASSERT_EQUAL(0, compare    ("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, insertBtree("11114", "aaaax"));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaax", BTREE));
        BFC_ASSERT_EQUAL(0, insertBtree("00001", "aaaax"));
        BFC_ASSERT_EQUAL(0, insertBtree("00002", "aaaax"));
        BFC_ASSERT_EQUAL(0, compare    ("11116", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, insertBtree("22222", "aaaax"));
        BFC_ASSERT_EQUAL(0, compare    ("11117", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11118", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("22222", "aaaax", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, 0));
    }

    void moveNextWhileInsertingTransactionTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn("11116", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn("11117", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn("11118", "aaaac"));

        BFC_ASSERT_EQUAL(0, compare    ("11111", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, insertTxn("11114", "aaaax"));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaax", TXN));
        BFC_ASSERT_EQUAL(0, insertTxn("00001", "aaaax"));
        BFC_ASSERT_EQUAL(0, insertTxn("00002", "aaaax"));
        BFC_ASSERT_EQUAL(0, compare    ("11116", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(0, insertTxn("22222", "aaaax"));
        BFC_ASSERT_EQUAL(0, compare    ("11117", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11118", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("22222", "aaaax", TXN));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, 0));
    }

    void moveNextWhileInsertingMixedTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11112", "aaaaa", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11117", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11118", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("11119", "aaaac"));

        BFC_ASSERT_EQUAL(0, compare    ("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11113", "xxxxx", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "xxxxx", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11117", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11118", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11119", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, 0));
    }

    void moveNextWhileErasingTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11114", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11115", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11116", "aaaaf"));

        BFC_ASSERT_EQUAL(0, compare    ("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, eraseTxn   ("11112"));
        BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(m_cursor)));
        BFC_ASSERT_EQUAL(true, 
                    bt_cursor_is_nil((ham_bt_cursor_t *)m_cursor));
        BFC_ASSERT_EQUAL(0, compare    ("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, eraseTxn   ("11114"));
        BFC_ASSERT_EQUAL(0, compare    ("11115", "aaaae", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11116", "aaaaf", TXN));
        BFC_ASSERT_EQUAL(0, eraseTxn   ("11116"));
        BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(m_cursor)));
        BFC_ASSERT_EQUAL(true, 
                    bt_cursor_is_nil((ham_bt_cursor_t *)m_cursor));
    }

    void movePreviousInEmptyTransactionTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a few keys into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    }

    void movePreviousInEmptyBtreeTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a few keys into the btree */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    }

    void movePreviousSmallerInTransactionTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a "small" key into the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        /* and a "large" one in the btree */
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    }

    void movePreviousSmallerInBtreeTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a "small" key into the btree */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* and a "large" one in the txn */
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    }

    void movePreviousSmallerInTransactionSequenceTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a few "small" keys into the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        ham_backend_t *be=db_get_backend(m_db);
        /* and a few "large" keys in the btree */
        key.data=(void *)"44444";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"55555";
        rec.data=(void *)"eeeee";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"66666";
        rec.data=(void *)"fffff";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("66666", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("fffff", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("55555", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("eeeee", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("44444", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    }

    void movePreviousSmallerInBtreeSequenceTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a few "small" keys into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* and a few "large" keys in the transaction */
        key.data=(void *)"44444";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        key.data=(void *)"55555";
        rec.data=(void *)"eeeee";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        key.data=(void *)"66666";
        rec.data=(void *)"fffff";
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("66666", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("fffff", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("55555", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("eeeee", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("44444", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    }

    void movePreviousOverErasedItemTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a few "small" keys into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* erase the one in the middle */
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_erase(m_db, m_txn, &key, 0));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    }

    void movePreviousOverIdenticalItemsTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        /* insert a few keys into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* overwrite the same keys in the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"22222";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"33333";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                    ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    }

    void moveBtreeThenPreviousOverIdenticalItemsTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        ham_backend_t *be=db_get_backend(m_db);
        /* insert a few keys into the btree */
        key.data=(void *)"00000";
        rec.data=(void *)"xxxxx";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* skip the first key, and overwrite all others in the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"22222";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"33333";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT(!(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN));
        BFC_ASSERT_EQUAL(0, strcmp("00000", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xxxxx", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    }

    void moveTxnThenPreviousOverIdenticalItemsTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        key.data=(void *)"00000";
        rec.data=(void *)"xxxxx";
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, m_txn, &key, &rec, 0));
        ham_backend_t *be=db_get_backend(m_db);
        /* insert a few keys into the btree */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* skip the first key, and overwrite all others in the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"22222";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"33333";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("00000", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xxxxx", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    }

    void movePreviousOverIdenticalItemsThenBtreeTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        ham_backend_t *be=db_get_backend(m_db);
        /* insert a few keys into the btree */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"99999";
        rec.data=(void *)"xxxxx";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        /* skip the first key, and overwrite all others in the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"22222";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"33333";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

        /* this moves the cursor to the first item */
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT(!(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN));
        BFC_ASSERT_EQUAL(0, strcmp("99999", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xxxxx", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    }

    void movePreviousOverIdenticalItemsThenTxnTest(void)
    {
        ham_key_t key={0}, key2={0};
        ham_record_t rec={0}, rec2={0};
        key.size=6;
        rec.size=6;

        ham_backend_t *be=db_get_backend(m_db);
        /* insert a few keys into the btree */
        key.data=(void *)"11111";
        rec.data=(void *)"aaaaa";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"22222";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"33333";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));
        key.data=(void *)"99999";
        rec.data=(void *)"xxxxx";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, 0));
        /* skip the first key, and overwrite all others in the transaction */
        key.data=(void *)"11111";
        rec.data=(void *)"bbbbb";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"22222";
        rec.data=(void *)"ccccc";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
        key.data=(void *)"33333";
        rec.data=(void *)"ddddd";
        BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

        /* this moves the cursor to the last item */
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("99999", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("xxxxx", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("33333", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ddddd", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("ccccc", (char *)rec2.data));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT(cursor_get_flags(m_cursor)&CURSOR_COUPLED_TO_TXN);
        BFC_ASSERT_EQUAL(0, strcmp("11111", (char *)key2.data));
        BFC_ASSERT_EQUAL(0, strcmp("bbbbb", (char *)rec2.data));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    }

    void movePreviousOverSequencesOfIdenticalItemsTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11113", "aaaaa", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11114", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11115", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("11116", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11117", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11118", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11116", "bbbba", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11117", "bbbbb", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11118", "bbbbc", HAM_OVERWRITE));

        BFC_ASSERT_EQUAL(0, comparePrev("11118", "bbbbc", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11117", "bbbbb", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11116", "bbbba", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11115", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11114", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11113", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, comparePrev(0, 0, 0));
    }

    void movePreviousWhileInsertingBtreeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("11116", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11117", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11118", "aaaac"));

        BFC_ASSERT_EQUAL(0, comparePrev("11118", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11117", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11116", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, insertBtree("11114", "aaaax"));
        BFC_ASSERT_EQUAL(0, comparePrev("11114", "aaaax", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11113", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, insertBtree("00000", "aaaax"));
        BFC_ASSERT_EQUAL(0, comparePrev("00000", "aaaax", BTREE));
        BFC_ASSERT_EQUAL(0, insertBtree("00001", "aaaax"));
        BFC_ASSERT_EQUAL(0, insertBtree("00002", "aaaax"));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, comparePrev(0, 0, 0));
    }

    void movePreviousWhileInsertingTransactionTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11116", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11117", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11118", "aaaac"));

        BFC_ASSERT_EQUAL(0, comparePrev("11118", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11117", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11116", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11114", "aaaax"));
        BFC_ASSERT_EQUAL(0, comparePrev("11114", "aaaax", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11113", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11112", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(0, insertTxn  ("00000", "aaaax"));
        BFC_ASSERT_EQUAL(0, comparePrev("00000", "aaaax", TXN));

        BFC_ASSERT_EQUAL(0, insertTxn  ("00001", "aaaax"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("00002", "aaaax"));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, comparePrev(0, 0, 0));
    }

    void movePreviousWhileInsertingMixedTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11112", "aaaaa", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11117", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11118", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("11119", "aaaac"));

        BFC_ASSERT_EQUAL(0, comparePrev("11119", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11118", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11117", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11113", "xxxxx", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, comparePrev("11113", "xxxxx", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11112", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, comparePrev(0, 0, 0));
    }

    void switchDirectionsInBtreeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("11114", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertBtree("11115", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertBtree("11116", "aaaaf"));
        BFC_ASSERT_EQUAL(0, insertBtree("11116", "aaaag", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertBtree("11117", "aaaah"));
        BFC_ASSERT_EQUAL(0, insertBtree("11118", "aaaai"));
        BFC_ASSERT_EQUAL(0, insertBtree("11119", "aaaaj"));

        BFC_ASSERT_EQUAL(0, compare    ("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaad", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11113", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaad", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11115", "aaaae", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11116", "aaaag", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11117", "aaaah", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11118", "aaaai", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11119", "aaaaj", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11118", "aaaai", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11117", "aaaah", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11116", "aaaag", BTREE));
    }

    void switchDirectionsInTransactionTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11114", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11115", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11116", "aaaaf"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11116", "aaaag", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11117", "aaaah"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11118", "aaaai"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11119", "aaaaj"));

        BFC_ASSERT_EQUAL(0, compare    ("11111", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaad", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11113", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11112", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaad", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11115", "aaaae", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11116", "aaaag", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11117", "aaaah", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11118", "aaaai", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11119", "aaaaj", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11118", "aaaai", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11117", "aaaah", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11116", "aaaag", TXN));
    }

    void switchDirectionsMixedStartInBtreeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11114", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertBtree("11115", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11116", "aaaaf"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11116", "aaaag", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertBtree("11117", "aaaah"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11118", "aaaai"));
        BFC_ASSERT_EQUAL(0, insertBtree("11119", "aaaaj"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11119", "aaaak", HAM_OVERWRITE));

        BFC_ASSERT_EQUAL(0, compare    ("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaad", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11113", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11112", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaad", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11115", "aaaae", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11116", "aaaag", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11117", "aaaah", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11118", "aaaai", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11119", "aaaak", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11118", "aaaai", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11117", "aaaah", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11116", "aaaag", TXN));
    }

    void switchDirectionsMixedStartInTxnTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("11114", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11115", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertBtree("11116", "aaaaf"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11116", "aaaag", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11117", "aaaah"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11118", "aaaai"));
        BFC_ASSERT_EQUAL(0, insertBtree("11119", "aaaaj"));

        BFC_ASSERT_EQUAL(0, compare    ("11111", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaad", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11113", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaad", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11115", "aaaae", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11116", "aaaag", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11117", "aaaah", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11118", "aaaai", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11119", "aaaaj", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11118", "aaaai", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11117", "aaaah", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11116", "aaaag", TXN));
    }

    void switchDirectionsMixedSequenceTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11112", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("11113", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("11114", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11113", "aaaae", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11114", "aaaaf", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11115", "aaaag", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11116", "aaaah"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11117", "aaaai"));
        BFC_ASSERT_EQUAL(0, insertBtree("11118", "aaaaj"));
        BFC_ASSERT_EQUAL(0, insertBtree("11119", "aaaak"));
        BFC_ASSERT_EQUAL(0, insertBtree("11120", "aaaal"));
        BFC_ASSERT_EQUAL(0, insertBtree("11121", "aaaam"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11120", "aaaan", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11121", "aaaao", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("11122", "aaaap"));

        BFC_ASSERT_EQUAL(0, compare    ("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaae", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaaf", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11113", "aaaae", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, comparePrev(0, 0, BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11112", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11113", "aaaae", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaaf", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11115", "aaaag", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11116", "aaaah", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11117", "aaaai", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11118", "aaaaj", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11119", "aaaak", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11120", "aaaan", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11121", "aaaao", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11122", "aaaap", TXN));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11122", "aaaap", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11121", "aaaao", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11120", "aaaan", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11119", "aaaak", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11118", "aaaaj", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11117", "aaaai", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11116", "aaaah", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11115", "aaaag", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11114", "aaaaf", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11113", "aaaae", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11114", "aaaaf", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11115", "aaaag", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11116", "aaaah", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11117", "aaaai", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11118", "aaaaj", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11119", "aaaak", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("11120", "aaaan", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11121", "aaaao", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("11122", "aaaap", TXN));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, BTREE));
    }

    void findTxnThenMoveNextTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("44444", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertBtree("55555", "aaaae"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"33333";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, compare    ("44444", "aaaad", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("55555", "aaaae", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, BTREE));
    }

    void findTxnThenMoveNext2Test(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("44444", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertBtree("55555", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertBtree("66666", "aaaaf"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("77777", "aaaag"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"44444";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, compare    ("55555", "aaaae", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("66666", "aaaaf", BTREE));
        BFC_ASSERT_EQUAL(0, compare    ("77777", "aaaag", TXN));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, BTREE));
    }

    void findTxnThenMovePreviousTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("44444", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertBtree("55555", "aaaae"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"33333";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, comparePrev("22222", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, comparePrev(0, 0, BTREE));
    }

    void findTxnThenMoveNext3Test(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaad", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("44444", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("55555", "aaaaf"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"33333";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, compare("44444", "aaaae", TXN));
        BFC_ASSERT_EQUAL(0, compare("55555", "aaaaf", TXN));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, TXN));
    }

    void findTxnThenMoveNext4Test(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaad", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertBtree("44444", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertBtree("55555", "aaaaf"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"33333";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, compare("44444", "aaaae", BTREE));
        BFC_ASSERT_EQUAL(0, compare("55555", "aaaaf", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, TXN));
    }

    void findTxnThenMovePrevious2Test(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("44444", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertBtree("55555", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertBtree("66666", "aaaaf"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("77777", "aaaag"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"44444";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, comparePrev("33333", "aaaac", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("22222", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, comparePrev(0, 0, BTREE));
    }

    void findTxnThenMovePrevious3Test(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaad", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertBtree("44444", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertBtree("55555", "aaaaf"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"33333";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, comparePrev("22222", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, comparePrev(0, 0, TXN));
    }

    void findTxnThenMovePrevious4Test(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaad", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertBtree("44444", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertBtree("55555", "aaaaf"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"33333";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, comparePrev("22222", "aaaab", BTREE));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, comparePrev(0, 0, TXN));
    }

    void findBtreeThenMoveNextTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("44444", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("55555", "aaaae"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"33333";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, compare    ("44444", "aaaad", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("55555", "aaaae", TXN));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, TXN));
    }

    void findBtreeThenMovePreviousTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("44444", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("55555", "aaaae"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"33333";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, comparePrev("22222", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", TXN));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, comparePrev(0, 0, TXN));
    }

    void findBtreeThenMovePrevious2Test(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("44444", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("55555", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("66666", "aaaaf"));
        BFC_ASSERT_EQUAL(0, insertBtree("77777", "aaaag"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"44444";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, comparePrev("33333", "aaaac", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("22222", "aaaab", TXN));
        BFC_ASSERT_EQUAL(0, comparePrev("11111", "aaaaa", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, comparePrev(0, 0, BTREE));
    }

    void findBtreeThenMoveNext2Test(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertBtree("44444", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("55555", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("66666", "aaaaf"));
        BFC_ASSERT_EQUAL(0, insertBtree("77777", "aaaag"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"44444";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, compare    ("55555", "aaaae", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("66666", "aaaaf", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("77777", "aaaag", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, BTREE));
    }

    void findBtreeThenMoveNext3Test(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaad", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertBtree("44444", "aaaae"));
        BFC_ASSERT_EQUAL(0, insertBtree("55555", "aaaaf"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"33333";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, compare("44444", "aaaae", BTREE));
        BFC_ASSERT_EQUAL(0, compare("55555", "aaaaf", BTREE));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, TXN));
    }

    void insertThenMoveNextTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("22222", "aaaab"));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaac"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("44444", "aaaad"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("55555", "aaaae"));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"33333";
        ham_record_t rec={0};
        rec.size=6;
        rec.data=(void *)"33333";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, compare    ("44444", "aaaad", TXN));
        BFC_ASSERT_EQUAL(0, compare    ("55555", "aaaae", TXN));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, compare(0, 0, TXN));
    }

    void abortWhileCursorActiveTest(void)
    {
        BFC_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_abort(m_txn, 0));
    }

    void commitWhileCursorActiveTest(void)
    {
        BFC_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_commit(m_txn, 0));
    }

    void eraseKeyWithTwoCursorsTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        ham_cursor_t *cursor2;
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_clone(m_cursor, &cursor2));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"11111";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(cursor2, &key, 0));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(m_cursor)));
        BFC_ASSERT_EQUAL(true, 
                    bt_cursor_is_nil((ham_bt_cursor_t *)m_cursor));
        BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(cursor2)));
        BFC_ASSERT_EQUAL(true, 
                    bt_cursor_is_nil((ham_bt_cursor_t *)cursor2));

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor2));
    }

    void eraseKeyWithTwoCursorsOverwriteTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        ham_cursor_t *cursor2;
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_clone(m_cursor, &cursor2));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"11111";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        ham_record_t rec={0};
        rec.size=6;
        rec.data=(void *)"11111";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(cursor2, &key, &rec, HAM_OVERWRITE));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(m_cursor)));
        BFC_ASSERT_EQUAL(true, 
                    bt_cursor_is_nil((ham_bt_cursor_t *)m_cursor));
        BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(cursor2)));
        BFC_ASSERT_EQUAL(true, 
                    bt_cursor_is_nil((ham_bt_cursor_t *)cursor2));

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor2));
    }

    void eraseWithThreeCursorsTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        ham_cursor_t *cursor2, *cursor3;
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_create(m_db, m_txn, 0, &cursor2));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_create(m_db, m_txn, 0, &cursor3));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"11111";
        ham_record_t rec={0};
        rec.size=6;
        rec.data=(void *)"33333";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(cursor2, &key, &rec, HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(cursor3, &key, &rec, HAM_OVERWRITE));

        BFC_ASSERT_EQUAL(0, 
                    ham_erase(m_db, m_txn, &key, 0));
        BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(m_cursor)));
        BFC_ASSERT_EQUAL(true, 
                    bt_cursor_is_nil((ham_bt_cursor_t *)m_cursor));
        BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(cursor2)));
        BFC_ASSERT_EQUAL(true, 
                    bt_cursor_is_nil((ham_bt_cursor_t *)cursor2));
        BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(cursor3)));
        BFC_ASSERT_EQUAL(true, 
                    bt_cursor_is_nil((ham_bt_cursor_t *)cursor3));

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor2));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor3));
    }

    void eraseKeyWithoutCursorsTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));
        ham_cursor_t *cursor2;
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_clone(m_cursor, &cursor2));

        ham_key_t key={0};
        key.size=6;
        key.data=(void *)"11111";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(cursor2, &key, 0));

        BFC_ASSERT_EQUAL(HAM_TXN_CONFLICT, 
                    ham_erase(m_db, 0, &key, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_erase(m_db, m_txn, &key, 0));
        BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(m_cursor)));
        BFC_ASSERT_EQUAL(true, 
                    bt_cursor_is_nil((ham_bt_cursor_t *)m_cursor));
        BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(cursor2)));
        BFC_ASSERT_EQUAL(true, 
                    bt_cursor_is_nil((ham_bt_cursor_t *)cursor2));

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor2));
    }

    void eraseKeyAndFlushTransactionsTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("11111", "aaaaa"));

        /* create a second txn, insert and commit, but do not flush the 
         * first one */
        ham_txn_t *txn2;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_db, 0));

        ham_cursor_t *cursor2;
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_create(m_db, txn2, 0, &cursor2));

        ham_key_t key={0};
        ham_record_t rec={0};
        key.size=6;
        key.data=(void *)"11112";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(cursor2, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_close(cursor2));

        /* commit the 2nd txn - it will not be flushed because an older
         * txn also was not flushed */
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));

        /* the other cursor is part of the first transaction; position on 
         * the new key */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));

        /* now erase the key */
        BFC_ASSERT_EQUAL(0, 
                    ham_erase(m_db, m_txn, &key, 0));

        /* cursor must be nil */
        BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(m_cursor)));
        BFC_ASSERT_EQUAL(true, 
                    bt_cursor_is_nil((ham_bt_cursor_t *)m_cursor));
    }
};

class DupeCacheTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

protected:
    ham_cursor_t *m_cursor;
    ham_db_t *m_db;
    ham_env_t *m_env;
    memtracker_t *m_alloc;

public:
    DupeCacheTest()
    : hamsterDB_fixture("DupeCacheTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(DupeCacheTest, createEmptyCloseTest);
        BFC_REGISTER_TEST(DupeCacheTest, createCapacityCloseTest);
        BFC_REGISTER_TEST(DupeCacheTest, appendTest);
        BFC_REGISTER_TEST(DupeCacheTest, insertAtBeginningTest);
        BFC_REGISTER_TEST(DupeCacheTest, insertAtEndTest);
        BFC_REGISTER_TEST(DupeCacheTest, insertMixedTest);
        BFC_REGISTER_TEST(DupeCacheTest, eraseAtBeginningTest);
        BFC_REGISTER_TEST(DupeCacheTest, eraseAtEndTest);
        BFC_REGISTER_TEST(DupeCacheTest, eraseMixedTest);
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
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &m_cursor));
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

    void createEmptyCloseTest(void)
    {
        dupecache_t c;
        BFC_ASSERT_EQUAL(0, dupecache_create(&c, m_cursor, 0));
        BFC_ASSERT_EQUAL(8u, dupecache_get_capacity(&c));
        BFC_ASSERT_EQUAL(0u, dupecache_get_count(&c));
        dupecache_clear(&c);
    }

    void createCapacityCloseTest(void)
    {
        dupecache_t c;
        BFC_ASSERT_EQUAL(0, dupecache_create(&c, m_cursor, 20));
        BFC_ASSERT_EQUAL(20u, dupecache_get_capacity(&c));
        BFC_ASSERT_EQUAL(0u, dupecache_get_count(&c));
        dupecache_clear(&c);
    }

    void appendTest(void)
    {
        dupecache_t c;
        BFC_ASSERT_EQUAL(0, dupecache_create(&c, m_cursor, 2));

        dupecache_line_t entries[20];
        memset(&entries[0], 0, sizeof(entries));
        for (int i=0; i<20; i++)
            dupecache_line_set_btree_rid(&entries[i], i);

        for (int i=0; i<20; i++)
            BFC_ASSERT_EQUAL(0, dupecache_append(&c, &entries[i]));
        BFC_ASSERT_EQUAL(32u, dupecache_get_capacity(&c));
        BFC_ASSERT_EQUAL(20u, dupecache_get_count(&c));

        dupecache_line_t *e=dupecache_get_elements(&c);
        for (int i=0; i<20; i++)
            BFC_ASSERT_EQUAL((ham_u64_t)i, dupecache_line_get_btree_rid(&e[i]));

        dupecache_clear(&c);
    }

    void insertAtBeginningTest(void)
    {
        dupecache_t c;
        BFC_ASSERT_EQUAL(0, dupecache_create(&c, m_cursor, 2));

        dupecache_line_t entries[20];
        memset(&entries[0], 0, sizeof(entries));
        for (int i=0; i<20; i++)
            dupecache_line_set_btree_rid(&entries[i], i);

        for (int i=0; i<20; i++)
            BFC_ASSERT_EQUAL(0, dupecache_insert(&c, 0, &entries[i]));
        BFC_ASSERT_EQUAL(32u, dupecache_get_capacity(&c));
        BFC_ASSERT_EQUAL(20u, dupecache_get_count(&c));

        dupecache_line_t *e=dupecache_get_elements(&c);
        for (int i=19, j=0; i>=0; i--, j++)
            BFC_ASSERT_EQUAL((ham_u64_t)i, dupecache_line_get_btree_rid(&e[j]));

        dupecache_clear(&c);
    }

    void insertAtEndTest(void)
    {
        dupecache_t c;
        BFC_ASSERT_EQUAL(0, dupecache_create(&c, m_cursor, 2));

        dupecache_line_t entries[20];
        memset(&entries[0], 0, sizeof(entries));
        for (int i=0; i<20; i++)
            dupecache_line_set_btree_rid(&entries[i], i);

        for (int i=0; i<20; i++)
            BFC_ASSERT_EQUAL(0, dupecache_insert(&c, i, &entries[i]));
        BFC_ASSERT_EQUAL(32u, dupecache_get_capacity(&c));
        BFC_ASSERT_EQUAL(20u, dupecache_get_count(&c));

        dupecache_line_t *e=dupecache_get_elements(&c);
        for (int i=0; i<20; i++)
            BFC_ASSERT_EQUAL((ham_u64_t)i, dupecache_line_get_btree_rid(&e[i]));

        dupecache_clear(&c);
    }

    void insertMixedTest(void)
    {
        dupecache_t c;
        BFC_ASSERT_EQUAL(0, dupecache_create(&c, m_cursor, 2));

        dupecache_line_t entries[20];
        memset(&entries[0], 0, sizeof(entries));
        for (int i=0; i<20; i++)
            dupecache_line_set_btree_rid(&entries[i], i);

        int p=0;
        for (int j=0; j<5; j++) {
            for (int i=0; i<4; i++) {
                BFC_ASSERT_EQUAL(0, dupecache_insert(&c, j, &entries[p++]));
            }
        }
        BFC_ASSERT_EQUAL(32u, dupecache_get_capacity(&c));
        BFC_ASSERT_EQUAL(20u, dupecache_get_count(&c));

        dupecache_line_t *e=dupecache_get_elements(&c);
        BFC_ASSERT_EQUAL((ham_u64_t)3,  e[ 0]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)7,  e[ 1]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)11, e[ 2]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)15, e[ 3]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)19, e[ 4]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)18, e[ 5]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)17, e[ 6]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)16, e[ 7]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)14, e[ 8]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)13, e[ 9]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)12, e[10]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)10, e[11]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)9,  e[12]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)8,  e[13]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)6,  e[14]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)5,  e[15]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)4,  e[16]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)2,  e[17]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)1,  e[18]._u._b._btree_rid);
        BFC_ASSERT_EQUAL((ham_u64_t)0,  e[19]._u._b._btree_rid);

        dupecache_clear(&c);
    }

    void eraseAtBeginningTest(void)
    {
        dupecache_t c;
        BFC_ASSERT_EQUAL(0, dupecache_create(&c, m_cursor, 2));

        dupecache_line_t entries[20];
        memset(&entries[0], 0, sizeof(entries));
        for (int i=0; i<20; i++)
            dupecache_line_set_btree_rid(&entries[i], i);

        for (int i=0; i<20; i++)
            BFC_ASSERT_EQUAL(0, dupecache_append(&c, &entries[i]));
        BFC_ASSERT_EQUAL(32u, dupecache_get_capacity(&c));
        BFC_ASSERT_EQUAL(20u, dupecache_get_count(&c));

        dupecache_line_t *e=dupecache_get_elements(&c);
        int s=1;
        for (int i=19; i>=0; i--) {
            BFC_ASSERT_EQUAL(0, dupecache_erase(&c, 0));
            BFC_ASSERT_EQUAL((unsigned)i, dupecache_get_count(&c));
            for (int j=0; j<i; j++)
                BFC_ASSERT_EQUAL((ham_u64_t)s+j, 
                            dupecache_line_get_btree_rid(&e[j]));
            s++;
        }

        BFC_ASSERT_EQUAL(32u, dupecache_get_capacity(&c));
        BFC_ASSERT_EQUAL(0u, dupecache_get_count(&c));
        dupecache_clear(&c);
    }

    void eraseAtEndTest(void)
    {
        dupecache_t c;
        BFC_ASSERT_EQUAL(0, dupecache_create(&c, m_cursor, 2));

        dupecache_line_t entries[20];
        memset(&entries[0], 0, sizeof(entries));
        for (int i=0; i<20; i++)
            dupecache_line_set_btree_rid(&entries[i], i);

        for (int i=0; i<20; i++)
            BFC_ASSERT_EQUAL(0, dupecache_append(&c, &entries[i]));
        BFC_ASSERT_EQUAL(32u, dupecache_get_capacity(&c));
        BFC_ASSERT_EQUAL(20u, dupecache_get_count(&c));

        dupecache_line_t *e=dupecache_get_elements(&c);
        for (int i=0; i<20; i++) {
            BFC_ASSERT_EQUAL(0, dupecache_erase(&c, dupecache_get_count(&c)-1));
            for (int j=0; j<20-i; j++)
                BFC_ASSERT_EQUAL((ham_u64_t)j,
                            dupecache_line_get_btree_rid(&e[j]));
        }

        BFC_ASSERT_EQUAL(32u, dupecache_get_capacity(&c));
        BFC_ASSERT_EQUAL(0u, dupecache_get_count(&c));
        dupecache_clear(&c);
    }

    void eraseMixedTest(void)
    {
        dupecache_t c;
        BFC_ASSERT_EQUAL(0, dupecache_create(&c, m_cursor, 2));

        dupecache_line_t entries[20];
        memset(&entries[0], 0, sizeof(entries));
        for (int i=0; i<20; i++)
            dupecache_line_set_btree_rid(&entries[i], i);

        for (int i=0; i<20; i++)
            BFC_ASSERT_EQUAL(0, dupecache_append(&c, &entries[i]));
        BFC_ASSERT_EQUAL(32u, dupecache_get_capacity(&c));
        BFC_ASSERT_EQUAL(20u, dupecache_get_count(&c));

        for (int i=0; i<10; i++)
            BFC_ASSERT_EQUAL(0, dupecache_erase(&c, i));

        dupecache_line_t *e=dupecache_get_elements(&c);
        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL((unsigned)i*2+1,
                            dupecache_line_get_btree_rid(&e[i]));
        }

        BFC_ASSERT_EQUAL(32u, dupecache_get_capacity(&c));
        BFC_ASSERT_EQUAL(10u, dupecache_get_count(&c));
        dupecache_clear(&c);
    }
};

class DupeCursorTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

protected:
    ham_cursor_t *m_cursor;
    ham_db_t *m_db;
    ham_env_t *m_env;
    ham_txn_t *m_txn;
    memtracker_t *m_alloc;

public:
    DupeCursorTest()
    : hamsterDB_fixture("DupeCursorTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(DupeCursorTest, simpleBtreeTest);
        BFC_REGISTER_TEST(DupeCursorTest, multipleBtreeTest);
        BFC_REGISTER_TEST(DupeCursorTest, simpleTxnInsertLastTest);
        BFC_REGISTER_TEST(DupeCursorTest, simpleTxnInsertFirstTest);
        BFC_REGISTER_TEST(DupeCursorTest, multipleTxnTest);
        BFC_REGISTER_TEST(DupeCursorTest, mixedTest);
        BFC_REGISTER_TEST(DupeCursorTest, findInDuplicatesTest);
        BFC_REGISTER_TEST(DupeCursorTest, cursorFindInDuplicatesTest);
        BFC_REGISTER_TEST(DupeCursorTest, skipDuplicatesTest);
        BFC_REGISTER_TEST(DupeCursorTest, txnInsertConflictTest);
        BFC_REGISTER_TEST(DupeCursorTest, txnEraseConflictTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseDuplicatesTest);
        BFC_REGISTER_TEST(DupeCursorTest, cloneDuplicateCursorTest);
        BFC_REGISTER_TEST(DupeCursorTest, insertCursorCouplesTest);
        BFC_REGISTER_TEST(DupeCursorTest, insertFirstTest);
        BFC_REGISTER_TEST(DupeCursorTest, insertLastTest);
        BFC_REGISTER_TEST(DupeCursorTest, insertAfterTest);
        BFC_REGISTER_TEST(DupeCursorTest, insertBeforeTest);
        BFC_REGISTER_TEST(DupeCursorTest, extendDupeCacheTest);
        BFC_REGISTER_TEST(DupeCursorTest, overwriteTxnDupeTest);
        BFC_REGISTER_TEST(DupeCursorTest, overwriteBtreeDupeTest);

        BFC_REGISTER_TEST(DupeCursorTest, eraseFirstTxnDupeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseSecondTxnDupeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseThirdTxnDupeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesTxnTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesMoveNextTxnTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesMovePreviousTxnTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesFindFirstTxnTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesFindLastTxnTest);

        BFC_REGISTER_TEST(DupeCursorTest, eraseFirstBtreeDupeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseSecondBtreeDupeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseThirdBtreeDupeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesBtreeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesMoveNextBtreeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesMovePreviousBtreeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesFindFirstBtreeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesFindLastBtreeTest);

        BFC_REGISTER_TEST(DupeCursorTest, eraseFirstMixedDupeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseSecondMixedDupeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseSecondMixedDupeTest2);
        BFC_REGISTER_TEST(DupeCursorTest, eraseThirdMixedDupeTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseThirdMixedDupeTest2);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesMixedTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesMixedTest2);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesMoveNextMixedTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesMoveNextMixedTest2);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesMoveNextMixedTest3);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesMovePreviousMixedTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesMovePreviousMixedTest2);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesMovePreviousMixedTest3);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesFindFirstMixedTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesFindFirstMixedTest2);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesFindFirstMixedTest3);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesFindLastMixedTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesFindLastMixedTest2);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAllDuplicatesFindLastMixedTest3);

        BFC_REGISTER_TEST(DupeCursorTest, eraseFirstTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseLastTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseAfterTest);
        BFC_REGISTER_TEST(DupeCursorTest, eraseBeforeTest);
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
                    HAM_ENABLE_DUPLICATES|HAM_ENABLE_TRANSACTIONS, 0664));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(m_env, m_db, 13, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&m_txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, m_txn, 0, &m_cursor));
    }

    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_cursor_close(m_cursor));
        if (m_txn)
            BFC_ASSERT_EQUAL(0, ham_txn_commit(m_txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_TXN_AUTO_COMMIT));
        BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
        ham_delete(m_db);
        ham_env_delete(m_env);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    ham_status_t insertBtree(const char *key, const char *rec, 
                        ham_u32_t flags=0)
    {
        ham_key_t k={0};
        k.data=(void *)key;
        k.size=strlen(key)+1;
        ham_record_t r={0};
        r.data=(void *)rec;
        r.size=strlen(rec)+1;

        ham_backend_t *be=db_get_backend(m_db);
        return (be->_fun_insert(be, &k, &r, flags));
    }

    ham_status_t eraseTxn(const char *key)
    {
        ham_key_t k={0};
        k.data=(void *)key;
        k.size=strlen(key)+1;

        return (ham_erase(m_db, m_txn, &k, 0));
    }

    ham_status_t move(const char *key, const char *rec, ham_u32_t flags,
                ham_cursor_t *cursor=0)
    {
        ham_key_t k={0};
        ham_record_t r={0};
        ham_status_t st;

        if (!cursor)
            cursor=m_cursor;

        st=ham_cursor_move(cursor, &k, &r, flags);
        if (st)
            return (st);
        if (strcmp(key, (char *)k.data))
            return (HAM_INTERNAL_ERROR);
        if (strcmp(rec, (char *)r.data))
            return (HAM_INTERNAL_ERROR);

        // now verify again, but with flags=0
        if (flags==0)
            return (0);
        st=ham_cursor_move(cursor, &k, &r, 0);
        if (st)
            return (st);
        if (strcmp(key, (char *)k.data))
            return (HAM_INTERNAL_ERROR);
        if (strcmp(rec, (char *)r.data))
            return (HAM_INTERNAL_ERROR);
        return (0);
    }

    ham_status_t find(const char *key, const char *rec)
    {
        ham_key_t k={0};
        ham_record_t r={0};
        ham_status_t st;

        st=ham_find(m_db, m_txn, &k, &r, 0);
        if (st)
            return (st);
        if (strcmp(key, (char *)k.data))
            return (HAM_INTERNAL_ERROR);
        if (strcmp(rec, (char *)r.data))
            return (HAM_INTERNAL_ERROR);

        return (0);
    }

    ham_status_t insertTxn(const char *key, const char *rec, 
                        ham_u32_t flags=0)
    {
        ham_key_t k={0};
        k.data=(void *)key;
        k.size=strlen(key)+1;
        ham_record_t r={0};
        r.data=(void *)rec;
        r.size=strlen(rec)+1;

        return (ham_cursor_insert(m_cursor, &k, &r, flags));
    }

    void simpleBtreeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaab", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaac", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaad", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaaa", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaab", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaac", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaad", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaad", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaac", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaab", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaaa", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void multipleBtreeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaab", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("33333", "aaaac", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaab", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("11111", "aaaac", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("44444", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertBtree("44444", "aaaab", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("44444", "aaaac", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, move       ("11111", "aaaaa", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("11111", "aaaab", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("11111", "aaaac", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaaa", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaab", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaac", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("44444", "aaaaa", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("44444", "aaaab", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("44444", "aaaac", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("44444", "aaaac", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("44444", "aaaab", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("44444", "aaaaa", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaac", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("44444", "aaaaa", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaac", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaab", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaaa", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("11111", "aaaac", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("11111", "aaaab", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("11111", "aaaaa", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void simpleTxnInsertLastTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaab", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaac", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaad", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaaa", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaab", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaac", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaad", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaad", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaac", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaab", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaaa", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void simpleTxnInsertFirstTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaab", 
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_FIRST));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaac", 
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_FIRST));
        BFC_ASSERT_EQUAL(0, insertTxn  ("33333", "aaaad", 
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_FIRST));

        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaad", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaac", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaab", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaaa", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaaa", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaab", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaac", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("33333", "aaaad", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void multipleTxnTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn("33333", "3aaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn("33333", "3aaab", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn("33333", "3aaac", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn("11111", "1aaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn("11111", "1aaab", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn("11111", "1aaac", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn("44444", "4aaaa"));
        BFC_ASSERT_EQUAL(0, insertTxn("44444", "4aaab", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn("44444", "4aaac", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, move     ("11111", "1aaaa", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move     ("11111", "1aaab", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move     ("11111", "1aaac", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move     ("33333", "3aaaa", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move     ("33333", "3aaab", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move     ("33333", "3aaac", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move     ("44444", "4aaaa", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move     ("44444", "4aaab", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move     ("44444", "4aaac", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move     ("44444", "4aaac", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move     ("44444", "4aaab", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move     ("44444", "4aaaa", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move     ("33333", "3aaac", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move     ("44444", "4aaaa", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move     ("33333", "3aaac", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move     ("33333", "3aaab", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move     ("33333", "3aaaa", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move     ("11111", "1aaac", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move     ("11111", "1aaab", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move     ("11111", "1aaaa", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void mixedTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k2", "r2.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k5", "r5.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.6", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k7", "r7.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k8", "r8.1"));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k2", "r2.1", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k2", "r2.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k3", "r3.1", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k3", "r3.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k3", "r3.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k4", "r4.1", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k4", "r4.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k4", "r4.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k5", "r5.1", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k5", "r5.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k5", "r5.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k5", "r5.4", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.1", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.4", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.5", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.6", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k7", "r7.1", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k7", "r7.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k7", "r7.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k7", "r7.4", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k8", "r8.1", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k8", "r8.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k7", "r7.4", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k7", "r7.3", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k7", "r7.2", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k7", "r7.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.6", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.5", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.4", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.3", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.2", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k5", "r5.4", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k5", "r5.3", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k5", "r5.2", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k5", "r5.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k4", "r4.3", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k4", "r4.2", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k4", "r4.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k3", "r3.3", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k3", "r3.2", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k3", "r3.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k2", "r2.2", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k2", "r2.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void findInDuplicatesTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k2", "r2.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k5", "r5.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.6", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k7", "r7.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k8", "r8.1"));

        ham_key_t key={0};
        ham_record_t rec={0};
        key.size=3;

        key.data=(void *)"k1";
        BFC_ASSERT_EQUAL(0, ham_find(m_db, m_txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r1.1"));

        key.data=(void *)"k2";
        BFC_ASSERT_EQUAL(0, ham_find(m_db, m_txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r2.1"));

        key.data=(void *)"k3";
        BFC_ASSERT_EQUAL(0, ham_find(m_db, m_txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r3.1"));

        key.data=(void *)"k4";
        BFC_ASSERT_EQUAL(0, ham_find(m_db, m_txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r4.1"));

        key.data=(void *)"k5";
        BFC_ASSERT_EQUAL(0, ham_find(m_db, m_txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r5.1"));

        key.data=(void *)"k6";
        BFC_ASSERT_EQUAL(0, ham_find(m_db, m_txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r6.1"));

        key.data=(void *)"k7";
        BFC_ASSERT_EQUAL(0, ham_find(m_db, m_txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r7.1"));

        key.data=(void *)"k8";
        BFC_ASSERT_EQUAL(0, ham_find(m_db, m_txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r8.1"));
    }

    void cursorFindInDuplicatesTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k2", "r2.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k5", "r5.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.6", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k7", "r7.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k8", "r8.1"));

        ham_key_t key={0};
        ham_record_t rec={0};
        key.size=3;

        key.data=(void *)"k1";
        BFC_ASSERT_EQUAL(0, ham_cursor_find_ex(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r1.1"));

        key.data=(void *)"k2";
        BFC_ASSERT_EQUAL(0, ham_cursor_find_ex(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r2.1"));

        key.data=(void *)"k3";
        BFC_ASSERT_EQUAL(0, ham_cursor_find_ex(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r3.1"));

        key.data=(void *)"k4";
        BFC_ASSERT_EQUAL(0, ham_cursor_find_ex(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r4.1"));

        key.data=(void *)"k5";
        BFC_ASSERT_EQUAL(0, ham_cursor_find_ex(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r5.1"));

        key.data=(void *)"k6";
        BFC_ASSERT_EQUAL(0, ham_cursor_find_ex(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r6.1"));

        key.data=(void *)"k7";
        BFC_ASSERT_EQUAL(0, ham_cursor_find_ex(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r7.1"));

        key.data=(void *)"k8";
        BFC_ASSERT_EQUAL(0, ham_cursor_find_ex(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r8.1"));
    }

    void skipDuplicatesTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k2", "r2.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k5", "r5.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.6", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k7", "r7.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k8", "r8.1"));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", 
                    HAM_CURSOR_FIRST|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k2", "r2.1",
                    HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k3", "r3.1",
                    HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k4", "r4.1",
                    HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k5", "r5.1",
                    HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.1",
                    HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k7", "r7.1",
                    HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k8", "r8.1",
                    HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0,
                    HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k8", "r8.1",
                    HAM_CURSOR_LAST|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k7", "r7.4",
                    HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k6", "r6.6",
                    HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k5", "r5.4",
                    HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k4", "r4.3",
                    HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k3", "r3.3",
                    HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k2", "r2.2",
                    HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1",
                    HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0,
                    HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
    }

    void txnInsertConflictTest(void)
    {
        ham_txn_t *txn1, *txn2;
        ham_key_t key;
        memset(&key, 0, sizeof(key));
        key.data=(void *)"hello";
        key.size=5;
        ham_record_t rec;
        memset(&rec, 0, sizeof(rec));

        ham_cursor_t *c;

        /* begin(T1); begin(T2); insert(T1, a); find(T2, a) -> conflict */
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, txn2, 0, &c));
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, txn1, &key, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_TXN_CONFLICT, ham_cursor_find(c, &key, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
    }

    void txnEraseConflictTest(void)
    {
        ham_txn_t *txn1, *txn2;
        ham_key_t key;
        memset(&key, 0, sizeof(key));
        key.data=(void *)"hello";
        key.size=5;
        ham_record_t rec;
        memset(&rec, 0, sizeof(rec));

        ham_cursor_t *c;

        /* begin(T1); begin(T2); insert(T1, a); find(T2, a) -> conflict */
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, txn2, 0, &c));
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, txn1, &key, &rec, HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(HAM_TXN_CONFLICT, ham_erase(m_db, 0, &key, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
    }

    void eraseDuplicatesTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k2", "r2.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k3", "r3.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k4", "r4.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k5", "r5.1"));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k5", "r5.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k6", "r6.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k6", "r6.6", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k7", "r7.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k7", "r7.4", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k8", "r8.1"));

        BFC_ASSERT_EQUAL(0, eraseTxn   ("k1"));
        BFC_ASSERT_EQUAL(0, eraseTxn   ("k2"));
        BFC_ASSERT_EQUAL(0, eraseTxn   ("k3"));
        BFC_ASSERT_EQUAL(0, eraseTxn   ("k4"));
        BFC_ASSERT_EQUAL(0, eraseTxn   ("k5"));
        BFC_ASSERT_EQUAL(0, eraseTxn   ("k6"));
        BFC_ASSERT_EQUAL(0, eraseTxn   ("k7"));
        BFC_ASSERT_EQUAL(0, eraseTxn   ("k8"));

        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, find("k1", 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, find("k2", 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, find("k3", 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, find("k4", 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, find("k5", 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, find("k6", 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, find("k7", 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, find("k8", 0));
    }

    void cloneDuplicateCursorTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r2.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r3.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r3.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r3.3", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, move("k1", "r2.2", HAM_CURSOR_FIRST));

        ham_cursor_t *c;
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_clone(m_cursor, &c));

        ham_key_t key={0};
        ham_record_t rec={0};
        BFC_ASSERT_EQUAL(0, ham_cursor_move(c, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r2.2"));
        BFC_ASSERT_EQUAL(0, strcmp((char *)key.data, "k1"));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void insertCursorCouplesTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r2.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r3.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r3.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r3.3", HAM_DUPLICATE));

        ham_key_t key={0};
        ham_record_t rec={0};
        BFC_ASSERT_EQUAL(0, ham_cursor_move(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, "r3.3"));
        BFC_ASSERT_EQUAL(0, strcmp((char *)key.data, "k1"));
    }

    void insertFirstTest(void)
    {
        static int C=2;
        /* B 1 3     */
        /* T     5 7 */
        ham_cursor_t *c[C];
        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, m_txn, 0, &c[i]));

        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

        ham_key_t key={0};
        key.size=3;
        key.data=(void *)"k1";

        /* each cursor is positioned on a different duplicate */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));

        /* now insert a key at the beginning */
        ham_record_t rec={0};
        rec.size=5;
        rec.data=(void *)"r1.2";
        BFC_ASSERT_EQUAL(0, ham_cursor_insert(c[0], &key, &rec,
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_FIRST));

        /* now verify that the keys were inserted in the correct order */
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.5", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));

        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_close(c[i]));
    }

    void insertLastTest(void)
    {
        static int C=2;
        /* B 1 3     */
        /* T     5 7 */
        ham_cursor_t *c[C];
        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, m_txn, 0, &c[i]));

        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

        ham_key_t key={0};
        key.size=3;
        key.data=(void *)"k1";

        /* each cursor is positioned on a different duplicate */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));

        /* now insert a key at the beginning */
        ham_record_t rec={0};
        rec.size=5;
        rec.data=(void *)"r1.2";
        BFC_ASSERT_EQUAL(0, ham_cursor_insert(c[0], &key, &rec,
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_LAST));

        /* now verify that the keys were inserted in the correct order */
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.5", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_LAST));

        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_close(c[i]));
    }

    void insertAfterTest(void)
    {
        static int C=4;
        /* B 1 3     */
        /* T     5 7 */
        ham_cursor_t *c[C];
        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, m_txn, 0, &c[i]));

        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

        ham_key_t key={0};
        key.size=3;
        key.data=(void *)"k1";

        /* each cursor is positioned on a different duplicate */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[1], &key, 0, HAM_CURSOR_NEXT));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[2], &key, 0, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[2], &key, 0, HAM_CURSOR_PREVIOUS));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[3], &key, 0, HAM_CURSOR_LAST));

        /* now insert keys in-between */
        ham_record_t rec={0};
        rec.size=5;
        ham_cursor_t *clone;
        rec.data=(void *)"r1.2";
        BFC_ASSERT_EQUAL(0, ham_cursor_clone(c[0], &clone));
        BFC_ASSERT_EQUAL(0, ham_cursor_insert(clone, &key, &rec,
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_AFTER));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));

        rec.data=(void *)"r1.4";
        BFC_ASSERT_EQUAL(0, ham_cursor_clone(c[1], &clone));
        BFC_ASSERT_EQUAL(0, ham_cursor_insert(clone, &key, &rec,
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_AFTER));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));

        rec.data=(void *)"r1.6";
        BFC_ASSERT_EQUAL(0, ham_cursor_clone(c[2], &clone));
        BFC_ASSERT_EQUAL(0, ham_cursor_insert(clone, &key, &rec,
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_AFTER));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));

        rec.data=(void *)"r1.8";
        BFC_ASSERT_EQUAL(0, ham_cursor_clone(c[3], &clone));
        BFC_ASSERT_EQUAL(0, ham_cursor_insert(clone, &key, &rec,
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_AFTER));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));

        /* now verify that the original 4 cursors are still coupled to the
         * same duplicate */
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", 0, c[0]));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", 0, c[1]));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.5", 0, c[2]));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", 0, c[3]));

        /* now verify that the keys were inserted in the correct order */
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.4", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.5", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.6", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.8", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));

        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_close(c[i]));
    }

    void insertBeforeTest(void)
    {
        const int C=4;
        /* B 1 3     */
        /* T     5 7 */
        ham_cursor_t *c[C];
        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, m_txn, 0, &c[i]));

        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

        ham_key_t key={0};
        key.size=3;
        key.data=(void *)"k1";

        /* each cursor is positioned on a different duplicate */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[1], &key, 0, HAM_CURSOR_NEXT));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[2], &key, 0, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[2], &key, 0, HAM_CURSOR_PREVIOUS));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[3], &key, 0, HAM_CURSOR_LAST));

        /* now insert keys in-between */
        ham_record_t rec={0};
        rec.size=5;
        ham_cursor_t *clone;
        rec.data=(void *)"r1.0";
        BFC_ASSERT_EQUAL(0, ham_cursor_clone(c[0], &clone));
        BFC_ASSERT_EQUAL(0, ham_cursor_insert(clone, &key, &rec,
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_BEFORE));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));

        rec.data=(void *)"r1.2";
        BFC_ASSERT_EQUAL(0, ham_cursor_clone(c[1], &clone));
        BFC_ASSERT_EQUAL(0, ham_cursor_insert(clone, &key, &rec,
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_BEFORE));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));

        rec.data=(void *)"r1.4";
        BFC_ASSERT_EQUAL(0, ham_cursor_clone(c[2], &clone));
        BFC_ASSERT_EQUAL(0, ham_cursor_insert(clone, &key, &rec,
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_BEFORE));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));

        rec.data=(void *)"r1.6";
        BFC_ASSERT_EQUAL(0, ham_cursor_clone(c[3], &clone));
        BFC_ASSERT_EQUAL(0, ham_cursor_insert(clone, &key, &rec,
                    HAM_DUPLICATE|HAM_DUPLICATE_INSERT_BEFORE));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));

        /* now verify that the original 4 cursors are still coupled to the
         * same duplicate */
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", 0, c[0]));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", 0, c[1]));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.5", 0, c[2]));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", 0, c[3]));

        /* now verify that the keys were inserted in the correct order */
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.0", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.4", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.5", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.6", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));

        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_close(c[i]));
    }

    void extendDupeCacheTest(void)
    {
        const int MAX=512;
        int i=0;

        for (; i<MAX/2; i++) {
            char buf[20];
            sprintf(buf, "%d", i);
            BFC_ASSERT_EQUAL(0, insertBtree("k1", buf, HAM_DUPLICATE));
        }

        for (; i<MAX; i++) {
            char buf[20];
            sprintf(buf, "%d", i);
            BFC_ASSERT_EQUAL(0, insertTxn  ("k1", buf, HAM_DUPLICATE));
        }

        ham_key_t key={0};
        key.size=3;
        key.data=(void *)"k1";

        ham_record_t rec={0};

        for (i=0; i<MAX; i++) {
            char buf[20];
            sprintf(buf, "%d", i);
            rec.data=(void *)&buf[0];
            rec.size=strlen(buf)+1;
            BFC_ASSERT_EQUAL(0, move("k1", buf, 
                    i==0 ? HAM_CURSOR_FIRST : HAM_CURSOR_NEXT));
        }
    }

    void overwriteTxnDupeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        ham_record_t rec={0};
        rec.size=5;

        rec.data=(void *)"r2.1";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_overwrite(m_cursor, &rec, 0));

        rec.data=(void *)"r2.2";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_overwrite(m_cursor, &rec, 0));

        rec.data=(void *)"r2.3";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_overwrite(m_cursor, &rec, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r2.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r2.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r2.3", HAM_CURSOR_NEXT));
    }

    void overwriteBtreeDupeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));

        ham_record_t rec={0};
        rec.size=5;

        rec.data=(void *)"r2.1";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_overwrite(m_cursor, &rec, 0));

        rec.data=(void *)"r2.2";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_overwrite(m_cursor, &rec, 0));

        rec.data=(void *)"r2.3";
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_overwrite(m_cursor, &rec, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r2.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r2.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r2.3", HAM_CURSOR_NEXT));
    }

    void eraseFirstTxnDupeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseSecondTxnDupeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseThirdTxnDupeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesTxnTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_LAST));
    }

    void eraseAllDuplicatesMoveNextTxnTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.1", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesMovePreviousTxnTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k0", "r0.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesFindFirstTxnTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.1", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            ham_key_t key={0};
            key.size=3;
            key.data=(void *)"k1";
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesFindLastTxnTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertTxn  ("k0", "r0.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            ham_key_t key={0};
            key.size=3;
            key.data=(void *)"k1";
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseFirstBtreeDupeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseSecondBtreeDupeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseThirdBtreeDupeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesBtreeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_LAST));
    }

    void eraseAllDuplicatesMoveNextBtreeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", 0));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k2", "r2.1", 0));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesMovePreviousBtreeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k0", "r0.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesFindFirstBtreeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k2", "r2.1", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            ham_key_t key={0};
            key.size=3;
            key.data=(void *)"k1";
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesFindLastBtreeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k0", "r0.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            ham_key_t key={0};
            key.size=3;
            key.data=(void *)"k1";
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseFirstMixedDupeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseSecondMixedDupeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseSecondMixedDupeTest2(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseThirdMixedDupeTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseThirdMixedDupeTest2(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.2", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesMixedTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_LAST));
    }

    void eraseAllDuplicatesMixedTest2(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_LAST));
    }

    void eraseAllDuplicatesMoveNextMixedTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", 0));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.1", 0));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesMoveNextMixedTest2(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", 0));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.1", 0));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesMoveNextMixedTest3(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", 0));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.1", 0));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesMovePreviousMixedTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k0", "r0.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesMovePreviousMixedTest2(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k0", "r0.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesMovePreviousMixedTest3(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k0", "r0.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesFindFirstMixedTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.1", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            ham_key_t key={0};
            key.size=3;
            key.data=(void *)"k1";
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesFindFirstMixedTest2(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.1", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            ham_key_t key={0};
            key.size=3;
            key.data=(void *)"k1";
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesFindFirstMixedTest3(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k2", "r2.1", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            ham_key_t key={0};
            key.size=3;
            key.data=(void *)"k1";
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k2", "r2.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesFindLastMixedTest(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k0", "r0.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            ham_key_t key={0};
            key.size=3;
            key.data=(void *)"k1";
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesFindLastMixedTest2(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k0", "r0.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            ham_key_t key={0};
            key.size=3;
            key.data=(void *)"k1";
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseAllDuplicatesFindLastMixedTest3(void)
    {
        BFC_ASSERT_EQUAL(0, insertBtree("k0", "r0.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

        for (int i=0; i<3; i++) {
            ham_key_t key={0};
            key.size=3;
            key.data=(void *)"k1";
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));
        }

        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move("k0", "r0.1", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));
    }

    void eraseFirstTest(void)
    {
        static int C=2;
        /* B 1 3     */
        /* T     5 7 */
        ham_cursor_t *c[C];
        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, m_txn, 0, &c[i]));

        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

        ham_key_t key={0};
        key.size=3;
        key.data=(void *)"k1";

        /* each cursor is positioned on a different duplicate */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));

        /* now erase the first key */
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c[0], 0));

        /* now verify that the keys were inserted in the correct order */
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.5", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.5", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));

        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_close(c[i]));
    }

    void eraseLastTest(void)
    {
        static int C=2;
        /* B 1 3     */
        /* T     5 7 */
        ham_cursor_t *c[C];
        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, m_txn, 0, &c[i]));

        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

        ham_key_t key={0};
        key.size=3;
        key.data=(void *)"k1";

        /* each cursor is positioned on a different duplicate */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[0], &key, 0, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[1], &key, 0, HAM_CURSOR_LAST));

        /* now erase the key */
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c[0], 0));

        /* now verify that the keys were inserted in the correct order */
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.5", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.5", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));

        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_close(c[i]));
    }

    void eraseAfterTest(void)
    {
        static int C=4;
        /* B 1 3     */
        /* T     5 7 */
        ham_cursor_t *c[C];
        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, m_txn, 0, &c[i]));

        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

        ham_key_t key={0};
        key.size=3;
        key.data=(void *)"k1";

        /* each cursor is positioned on a different duplicate */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[1], &key, 0, HAM_CURSOR_NEXT));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[2], &key, 0, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[2], &key, 0, HAM_CURSOR_PREVIOUS));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[3], &key, 0, HAM_CURSOR_LAST));

        /* now erase the second key */
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c[1], 0));

        /* now verify that the other 3 cursors are still coupled to the
         * same duplicate */
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", 0, c[0]));
        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, move("k1", "r1.3", 0, c[1]));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.5", 0, c[2]));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", 0, c[3]));

        /* now verify that the keys were inserted in the correct order */
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.5", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));

        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_close(c[i]));
    }

    void eraseBeforeTest(void)
    {
        const int C=4;
        /* B 1 3     */
        /* T     5 7 */
        ham_cursor_t *c[C];
        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, m_txn, 0, &c[i]));

        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.1"));
        BFC_ASSERT_EQUAL(0, insertBtree("k1", "r1.3", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

        ham_key_t key={0};
        key.size=3;
        key.data=(void *)"k1";

        /* each cursor is positioned on a different duplicate */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[1], &key, 0, HAM_CURSOR_NEXT));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[2], &key, 0, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[2], &key, 0, HAM_CURSOR_PREVIOUS));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[3], &key, 0, HAM_CURSOR_LAST));

        /* erase the 3rd key */
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c[2], 0));

        /* now verify that the other 3 cursors are still coupled to the
         * same duplicate */
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", 0, c[0]));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", 0, c[1]));
        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, move("k1", "r1.5", 0, c[2]));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", 0, c[3]));

        /* now verify that the keys were inserted in the correct order */
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.7", HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.3", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(0, move       ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, move(0, 0, HAM_CURSOR_PREVIOUS));

        for (int i=0; i<C; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_close(c[i]));
    }
};

BFC_REGISTER_FIXTURE(TempTxnCursorTest);
BFC_REGISTER_FIXTURE(LongTxnCursorTest);
BFC_REGISTER_FIXTURE(NoTxnCursorTest);
BFC_REGISTER_FIXTURE(DupeCacheTest);
BFC_REGISTER_FIXTURE(DupeCursorTest);

