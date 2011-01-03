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
#include "../src/txn_cursor.h"
#include "../src/page.h"
#include "../src/error.h"
#include "../src/env.h"
#include "../src/freelist.h"
#include "../src/os.h"
#include "memtracker.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

class TxnCursorTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    TxnCursorTest()
    : hamsterDB_fixture("TxnCursorTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(TxnCursorTest, structureTest);
        BFC_REGISTER_TEST(TxnCursorTest, cursorIsNilTest);
        BFC_REGISTER_TEST(TxnCursorTest, txnOpLinkedListTest);
        BFC_REGISTER_TEST(TxnCursorTest, getKeyFromCoupledCursorTest);
        BFC_REGISTER_TEST(TxnCursorTest, getKeyFromCoupledCursorUserAllocTest);
        BFC_REGISTER_TEST(TxnCursorTest, getKeyFromCoupledCursorEmptyKeyTest);
        BFC_REGISTER_TEST(TxnCursorTest, getKeyFromUncoupledCursorTest);
        BFC_REGISTER_TEST(TxnCursorTest, getKeyFromNilCursorTest);
        BFC_REGISTER_TEST(TxnCursorTest, getRecordFromCoupledCursorTest);
        BFC_REGISTER_TEST(TxnCursorTest, getRecordFromCoupledCursorUserAllocTest);
        BFC_REGISTER_TEST(TxnCursorTest, getRecordFromCoupledCursorEmptyRecordTest);
        BFC_REGISTER_TEST(TxnCursorTest, getRecordFromUncoupledCursorTest);
        BFC_REGISTER_TEST(TxnCursorTest, getRecordFromNilCursorTest);
    }

protected:
    ham_db_t *m_db;
    ham_env_t *m_env;
    memtracker_t *m_alloc;

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
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(m_env, 0));
        ham_delete(m_db);
        ham_env_delete(m_env);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void structureTest(void)
    {
        txn_cursor_t cursor={0};
        ham_key_t k={0};
        txn_op_t op={0};

        BFC_ASSERT_EQUAL((ham_db_t *)0, txn_cursor_get_db(&cursor));
        txn_cursor_set_db(&cursor, m_db);
        BFC_ASSERT_EQUAL(m_db, txn_cursor_get_db(&cursor));
        txn_cursor_set_db(&cursor, 0);

        BFC_ASSERT_EQUAL(0u, txn_cursor_get_flags(&cursor));
        txn_cursor_set_flags(&cursor, 0x345);
        BFC_ASSERT_EQUAL(0x345u, txn_cursor_get_flags(&cursor));
        txn_cursor_set_flags(&cursor, 0);

        BFC_ASSERT_EQUAL((ham_key_t *)0, txn_cursor_get_uncoupled_key(&cursor));
        txn_cursor_set_uncoupled_key(&cursor, &k);
        BFC_ASSERT_EQUAL(&k, txn_cursor_get_uncoupled_key(&cursor));
        txn_cursor_set_uncoupled_key(&cursor, 0);

        BFC_ASSERT_EQUAL((txn_op_t *)0, txn_cursor_get_coupled_op(&cursor));
        txn_cursor_set_coupled_op(&cursor, &op);
        BFC_ASSERT_EQUAL(&op, txn_cursor_get_coupled_op(&cursor));
        txn_cursor_set_coupled_op(&cursor, 0);

        BFC_ASSERT_EQUAL((txn_cursor_t *)0, 
                    txn_cursor_get_coupled_next(&cursor));
        txn_cursor_set_coupled_next(&cursor, (txn_cursor_t *)0x12);
        BFC_ASSERT_EQUAL((txn_cursor_t *)0x12,
                    txn_cursor_get_coupled_next(&cursor));
        txn_cursor_set_coupled_next(&cursor, 0);

        BFC_ASSERT_EQUAL((txn_cursor_t *)0, 
                    txn_cursor_get_coupled_previous(&cursor));
        txn_cursor_set_coupled_previous(&cursor, (txn_cursor_t *)0x21);
        BFC_ASSERT_EQUAL((txn_cursor_t *)0x21,
                    txn_cursor_get_coupled_previous(&cursor));
        txn_cursor_set_coupled_previous(&cursor, 0);
    }

    void cursorIsNilTest(void)
    {
        txn_cursor_t cursor={0};
        txn_cursor_set_db(&cursor, m_db);

        BFC_ASSERT_EQUAL(HAM_TRUE, txn_cursor_is_nil(&cursor));
        txn_cursor_set_flags(&cursor, TXN_CURSOR_FLAG_COUPLED);
        BFC_ASSERT_EQUAL(HAM_FALSE, txn_cursor_is_nil(&cursor));
        txn_cursor_set_to_nil(&cursor);
        BFC_ASSERT_EQUAL(HAM_TRUE, txn_cursor_is_nil(&cursor));

        txn_cursor_set_flags(&cursor, TXN_CURSOR_FLAG_UNCOUPLED);
        BFC_ASSERT_EQUAL(HAM_FALSE, txn_cursor_is_nil(&cursor));
        txn_cursor_set_to_nil(&cursor);
        BFC_ASSERT_EQUAL(HAM_TRUE, txn_cursor_is_nil(&cursor));

        BFC_ASSERT_EQUAL(0u, txn_cursor_get_flags(&cursor));
    }

    void txnOpLinkedListTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t key={0};
        ham_record_t record={0};
        key.data=(void *)"hello";
        key.size=5;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c1={0};
        txn_cursor_set_flags(&c1, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_coupled_op(&c1, op);
        txn_cursor_t c2=c1;
        txn_cursor_t c3=c1;
        
        BFC_ASSERT_EQUAL((txn_cursor_t *)0, txn_op_get_cursors(op));

        txn_op_add_cursor(op, &c1);
        BFC_ASSERT_EQUAL(&c1, txn_op_get_cursors(op));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0, 
                    txn_cursor_get_coupled_previous(&c1));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0, 
                    txn_cursor_get_coupled_next(&c1));

        txn_op_add_cursor(op, &c2);
        BFC_ASSERT_EQUAL(&c2, txn_op_get_cursors(op));
        BFC_ASSERT_EQUAL(&c2,
                    txn_cursor_get_coupled_previous(&c1));
        BFC_ASSERT_EQUAL(&c1,
                    txn_cursor_get_coupled_next(&c2));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0,
                    txn_cursor_get_coupled_previous(&c2));

        txn_op_add_cursor(op, &c3);
        BFC_ASSERT_EQUAL(&c3, txn_op_get_cursors(op));
        BFC_ASSERT_EQUAL(&c3,
                    txn_cursor_get_coupled_previous(&c2));
        BFC_ASSERT_EQUAL(&c2,
                    txn_cursor_get_coupled_next(&c3));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0,
                    txn_cursor_get_coupled_previous(&c3));

        txn_op_remove_cursor(op, &c2);
        BFC_ASSERT_EQUAL(&c3, txn_op_get_cursors(op));
        BFC_ASSERT_EQUAL(&c3,
                    txn_cursor_get_coupled_previous(&c1));
        BFC_ASSERT_EQUAL(&c1,
                    txn_cursor_get_coupled_next(&c3));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0,
                    txn_cursor_get_coupled_next(&c1));

        txn_op_remove_cursor(op, &c3);
        BFC_ASSERT_EQUAL(&c1, txn_op_get_cursors(op));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0,
                    txn_cursor_get_coupled_previous(&c1));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0,
                    txn_cursor_get_coupled_next(&c1));

        txn_op_remove_cursor(op, &c1);
        BFC_ASSERT_EQUAL((txn_cursor_t *)0, txn_op_get_cursors(op));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getKeyFromCoupledCursorTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t k={0};
        ham_key_t key={0};
        ham_record_t record={0};
        key.data=(void *)"hello";
        key.size=5;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_db(&c, m_db);
        txn_cursor_set_coupled_op(&c, op);

        BFC_ASSERT_EQUAL(0, txn_cursor_get_key(&c, &k));
        BFC_ASSERT_EQUAL(k.size, key.size);
        BFC_ASSERT_EQUAL(0, memcmp(k.data, key.data, key.size));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getKeyFromCoupledCursorUserAllocTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t k={0};
        ham_key_t key={0};
        ham_record_t record={0};
        key.data=(void *)"hello";
        key.size=5;

        char buffer[1024]={0};
        k.data=&buffer[0];
        k.flags=HAM_KEY_USER_ALLOC;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_db(&c, m_db);
        txn_cursor_set_coupled_op(&c, op);

        BFC_ASSERT_EQUAL(0, txn_cursor_get_key(&c, &k));
        BFC_ASSERT_EQUAL(k.size, key.size);
        BFC_ASSERT_EQUAL(0, memcmp(k.data, key.data, key.size));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getKeyFromCoupledCursorEmptyKeyTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t k={0};
        ham_key_t key={0};
        ham_record_t record={0};

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_db(&c, m_db);
        txn_cursor_set_coupled_op(&c, op);

        BFC_ASSERT_EQUAL(0, txn_cursor_get_key(&c, &k));
        BFC_ASSERT_EQUAL(k.size, key.size);
        BFC_ASSERT_EQUAL((void *)0, k.data);

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getKeyFromUncoupledCursorTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t k={0};
        ham_key_t key={0};
        ham_record_t record={0};
        key.data=(void *)"hello";
        key.size=5;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_UNCOUPLED);
        txn_cursor_set_db(&c, m_db);
        txn_cursor_set_uncoupled_key(&c, &key);

        BFC_ASSERT_EQUAL(HAM_INTERNAL_ERROR, txn_cursor_get_key(&c, &k));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getKeyFromNilCursorTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t k={0};
        ham_key_t key={0};
        ham_record_t record={0};
        key.data=(void *)"hello";
        key.size=5;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, 0);
        txn_cursor_set_db(&c, m_db);

        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, txn_cursor_get_key(&c, &k));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getRecordFromCoupledCursorTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t key={0};
        ham_record_t r={0};
        ham_record_t record={0};
        record.data=(void *)"hello";
        record.size=5;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_db(&c, m_db);
        txn_cursor_set_coupled_op(&c, op);

        BFC_ASSERT_EQUAL(0, txn_cursor_get_record(&c, &r));
        BFC_ASSERT_EQUAL(r.size, record.size);
        BFC_ASSERT_EQUAL(0, memcmp(r.data, record.data, record.size));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getRecordFromCoupledCursorUserAllocTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t key={0};
        ham_record_t r={0};
        ham_record_t record={0};
        record.data=(void *)"hello";
        record.size=5;

        char buffer[1024]={0};
        r.data=&buffer[0];
        r.flags=HAM_RECORD_USER_ALLOC;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_db(&c, m_db);
        txn_cursor_set_coupled_op(&c, op);

        BFC_ASSERT_EQUAL(0, txn_cursor_get_record(&c, &r));
        BFC_ASSERT_EQUAL(r.size, record.size);
        BFC_ASSERT_EQUAL(0, memcmp(r.data, record.data, record.size));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getRecordFromCoupledCursorEmptyRecordTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t key={0};
        ham_record_t record={0};
        ham_record_t r={0};

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_db(&c, m_db);
        txn_cursor_set_coupled_op(&c, op);

        BFC_ASSERT_EQUAL(0, txn_cursor_get_record(&c, &r));
        BFC_ASSERT_EQUAL(r.size, record.size);
        BFC_ASSERT_EQUAL((void *)0, r.data);

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getRecordFromUncoupledCursorTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t key={0};
        ham_record_t record={0};
        ham_record_t r={0};

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_UNCOUPLED);
        txn_cursor_set_db(&c, m_db);
        txn_cursor_set_uncoupled_key(&c, &key);

        BFC_ASSERT_EQUAL(HAM_INTERNAL_ERROR, txn_cursor_get_record(&c, &r));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getRecordFromNilCursorTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t key={0};
        ham_record_t record={0};
        ham_record_t r={0};

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, 0);
        txn_cursor_set_db(&c, m_db);

        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, txn_cursor_get_record(&c, &r));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }
};

BFC_REGISTER_FIXTURE(TxnCursorTest);

