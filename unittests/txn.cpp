/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
#include "../src/env.h"
#include "../src/freelist.h"
#include "../src/os.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace ham;

class TxnTest : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  TxnTest()
    : hamsterDB_fixture("TxnTest") {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(TxnTest, checkIfLogCreatedTest);
    BFC_REGISTER_TEST(TxnTest, beginCommitTest);
    BFC_REGISTER_TEST(TxnTest, multipleBeginCommitTest);
    BFC_REGISTER_TEST(TxnTest, beginAbortTest);
    BFC_REGISTER_TEST(TxnTest, txnStructureTest);
    BFC_REGISTER_TEST(TxnTest, txnTreeStructureTest);
    BFC_REGISTER_TEST(TxnTest, txnMultipleTreesTest);
    BFC_REGISTER_TEST(TxnTest, txnNodeStructureTest);
    BFC_REGISTER_TEST(TxnTest, txnNodeCreatedOnceTest);
    BFC_REGISTER_TEST(TxnTest, txnMultipleNodesTest);
    BFC_REGISTER_TEST(TxnTest, txnOpStructureTest);
    BFC_REGISTER_TEST(TxnTest, txnMultipleOpsTest);
    BFC_REGISTER_TEST(TxnTest, txnInsertConflict1Test);
    BFC_REGISTER_TEST(TxnTest, txnInsertConflict2Test);
    BFC_REGISTER_TEST(TxnTest, txnInsertConflict3Test);
    BFC_REGISTER_TEST(TxnTest, txnInsertConflict4Test);
    BFC_REGISTER_TEST(TxnTest, txnInsertConflict5Test);
    BFC_REGISTER_TEST(TxnTest, txnInsertFind1Test);
    BFC_REGISTER_TEST(TxnTest, txnInsertFind2Test);
    BFC_REGISTER_TEST(TxnTest, txnInsertFind3Test);
    BFC_REGISTER_TEST(TxnTest, txnInsertFind4Test);
    BFC_REGISTER_TEST(TxnTest, txnInsertFind5Test);
    //BFC_REGISTER_TEST(TxnTest, txnPartialInsertFindTest);
    BFC_REGISTER_TEST(TxnTest, txnInsertFindErase1Test);
    BFC_REGISTER_TEST(TxnTest, txnInsertFindErase2Test);
    BFC_REGISTER_TEST(TxnTest, txnInsertFindErase3Test);
    BFC_REGISTER_TEST(TxnTest, txnInsertFindErase4Test);
  }

protected:
  ham_db_t *m_db;
  ham_env_t *m_env;
  Database *m_dbp;

public:
  virtual void setup() {
    __super::setup();

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_RECOVERY | HAM_ENABLE_TRANSACTIONS, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 13, HAM_ENABLE_DUPLICATES, 0));
    m_dbp = (Database *)m_db;
  }

  virtual void teardown() {
    __super::teardown();

    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void checkIfLogCreatedTest() {
    BFC_ASSERT(((Environment *)m_env)->get_log() != 0);
    BFC_ASSERT(m_dbp->get_rt_flags() & HAM_ENABLE_RECOVERY);
  }

  void beginCommitTest() {
    ham_txn_t *txn;

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void multipleBeginCommitTest() {
    ham_txn_t *txn1, *txn2, *txn3;

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn3, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL((Transaction *)0,
        ((Transaction *)txn1)->get_older());
    BFC_ASSERT_EQUAL((Transaction *)txn2,
        ((Transaction *)txn1)->get_newer());

    BFC_ASSERT_EQUAL((Transaction *)txn1,
        ((Transaction *)txn2)->get_older());
    BFC_ASSERT_EQUAL((Transaction *)txn3,
        ((Transaction *)txn2)->get_newer());

    BFC_ASSERT_EQUAL((Transaction *)txn2,
        ((Transaction *)txn3)->get_older());
    BFC_ASSERT_EQUAL((Transaction *)0,
        ((Transaction *)txn3)->get_newer());

    /* have to commit the txns in the same order as they were created,
     * otherwise env_flush_committed_txns() will not flush the oldest
     * transaction */
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));

    BFC_ASSERT_EQUAL((Transaction *)0,
        ((Transaction *)txn2)->get_older());
    BFC_ASSERT_EQUAL((Transaction *)txn3,
        ((Transaction *)txn2)->get_newer());
    BFC_ASSERT_EQUAL((Transaction *)txn2,
        ((Transaction *)txn3)->get_older());
    BFC_ASSERT_EQUAL((Transaction *)0,
        ((Transaction *)txn3)->get_newer());

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));

    BFC_ASSERT_EQUAL((Transaction *)0,
        ((Transaction *)txn3)->get_older());
    BFC_ASSERT_EQUAL((Transaction *)0,
        ((Transaction *)txn3)->get_newer());

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn3, 0));
  }

  void beginAbortTest() {
    ham_txn_t *txn;

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
  }

  void txnStructureTest() {
    Transaction *txn;

    BFC_ASSERT_EQUAL(0, ham_txn_begin((ham_txn_t **)&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(m_env, txn->get_env());
    BFC_ASSERT_EQUAL((ham_u64_t)1, txn->get_id());

    txn->set_flags(0x99);
    BFC_ASSERT_EQUAL((ham_u32_t)0x99, txn->get_flags());

    txn->set_log_desc(4);
    BFC_ASSERT_EQUAL(4, txn->get_log_desc());
    txn->set_log_desc(0);

    txn->set_oldest_op((txn_op_t *)2);
    BFC_ASSERT_EQUAL((txn_op_t *)2, txn->get_oldest_op());
    txn->set_oldest_op((txn_op_t *)0);

    txn->set_newest_op((txn_op_t *)2);
    BFC_ASSERT_EQUAL((txn_op_t *)2, txn->get_newest_op());
    txn->set_newest_op((txn_op_t *)0);

    txn->set_newer((Transaction *)1);
    BFC_ASSERT_EQUAL((Transaction *)1, txn->get_newer());
    txn->set_newer((Transaction *)0);

    txn->set_older((Transaction *)3);
    BFC_ASSERT_EQUAL((Transaction *)3, txn->get_older());
    txn->set_older((Transaction *)0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit((ham_txn_t *)txn, 0));
  }

  void txnTreeStructureTest() {
    ham_txn_t *txn;
    TransactionTree *tree;

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    tree = m_dbp->get_optree();
    BFC_ASSERT(tree != 0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void txnTreeCreatedOnceTest() {
    ham_txn_t *txn;
    TransactionTree *tree, *tree2;

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    tree = m_dbp->get_optree();
    BFC_ASSERT(tree != 0);
    tree2 = m_dbp->get_optree();
    BFC_ASSERT_EQUAL(tree, tree2);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void txnMultipleTreesTest() {
    ham_db_t *db2, *db3;
    ham_txn_t *txn;
    TransactionTree *tree1, *tree2, *tree3;

    BFC_ASSERT_EQUAL(0, ham_env_create_db(m_env, &db2, 14, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_env_create_db(m_env, &db3, 15, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    tree1 = m_dbp->get_optree();
    tree2 = ((Database *)db2)->get_optree();
    tree3 = ((Database *)db3)->get_optree();
    BFC_ASSERT(tree1 != 0);
    BFC_ASSERT(tree2 != 0);
    BFC_ASSERT(tree3 != 0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    BFC_ASSERT_EQUAL(0, ham_db_close(db2, 0));
    BFC_ASSERT_EQUAL(0, ham_db_close(db3, 0));
  }

  void txnNodeStructureTest() {
    ham_txn_t *txn;
    txn_opnode_t *node;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = txn_opnode_create(m_dbp, &key);
    ham_key_t *k = txn_opnode_get_key(node);
    BFC_ASSERT_EQUAL(k->size, key.size);
    BFC_ASSERT_EQUAL(0, memcmp(k->data, key.data, k->size));

    txn_opnode_set_oldest_op(node, (txn_op_t *)3);
    BFC_ASSERT_EQUAL((txn_op_t *)3, txn_opnode_get_oldest_op(node));
    txn_opnode_set_oldest_op(node, 0);

    txn_opnode_set_newest_op(node, (txn_op_t *)4);
    BFC_ASSERT_EQUAL((txn_op_t *)4, txn_opnode_get_newest_op(node));
    txn_opnode_set_newest_op(node, 0);

    /* need at least one txn_op_t structure in this node, otherwise
     * memory won't be cleaned up correctly */
    (void)txn_opnode_append((Transaction *)txn, node, 0,
        TXN_OP_INSERT_DUP, 55, &rec);
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void txnNodeCreatedOnceTest() {
    ham_txn_t *txn;
    txn_opnode_t *node, *node2;
    ham_key_t key1, key2;
    memset(&key1, 0, sizeof(key1));
    key1.data = (void *)"hello";
    key1.size = 5;
    memset(&key2, 0, sizeof(key2));
    key2.data = (void *)"world";
    key2.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = txn_opnode_create(m_dbp, &key1);
    BFC_ASSERT(node != 0);
    node2 = txn_opnode_get(m_dbp, &key1, 0);
    BFC_ASSERT_EQUAL(node, node2);
    node2 = txn_opnode_get(m_dbp, &key2, 0);
    BFC_ASSERT_EQUAL((txn_opnode_t *)NULL, node2);
    node2 = txn_opnode_create(m_dbp, &key2);
    BFC_ASSERT(node != node2);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void txnMultipleNodesTest() {
    ham_txn_t *txn;
    txn_opnode_t *node1, *node2, *node3;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"1111";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node1 = txn_opnode_create(m_dbp, &key);
    BFC_ASSERT(node1 != 0);
    key.data = (void *)"2222";
    node2 = txn_opnode_create(m_dbp, &key);
    BFC_ASSERT(node2 != 0);
    key.data = (void *)"3333";
    node3 = txn_opnode_create(m_dbp, &key);
    BFC_ASSERT(node3 != 0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void txnOpStructureTest() {
    ham_txn_t *txn;
    txn_opnode_t *node;
    txn_op_t *op, next;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t record;
    memset(&record, 0, sizeof(record));
    record.data = (void *)"world";
    record.size = 5;
    memset(&next, 0, sizeof(next));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = txn_opnode_create(m_dbp, &key);
    op = txn_opnode_append((Transaction *)txn, node, 0,
        TXN_OP_INSERT_DUP, 55, &record);
    BFC_ASSERT(op != 0);

    BFC_ASSERT_EQUAL(TXN_OP_INSERT_DUP, txn_op_get_flags(op));
    txn_op_set_flags(op, 13);
    BFC_ASSERT_EQUAL(13u, txn_op_get_flags(op));
    txn_op_set_flags(op, TXN_OP_INSERT_DUP);

    BFC_ASSERT_EQUAL(55ull, txn_op_get_lsn(op));
    txn_op_set_lsn(op, 23);
    BFC_ASSERT_EQUAL(23ull, txn_op_get_lsn(op));
    txn_op_set_lsn(op, 55);

    BFC_ASSERT_EQUAL((txn_op_t *)0, txn_op_get_next_in_node(op));
    txn_op_set_next_in_node(op, &next);
    BFC_ASSERT_EQUAL(&next, txn_op_get_next_in_node(op));
    txn_op_set_next_in_node(op, 0);

    BFC_ASSERT_EQUAL((txn_op_t *)0, txn_op_get_next_in_txn(op));
    txn_op_set_next_in_txn(op, &next);
    BFC_ASSERT_EQUAL(&next, txn_op_get_next_in_txn(op));
    txn_op_set_next_in_txn(op, 0);

    BFC_ASSERT_EQUAL((txn_cursor_t *)0, txn_op_get_cursors(op));
    txn_op_set_cursors(op, (txn_cursor_t *)0x43);
    BFC_ASSERT_EQUAL((txn_cursor_t *)0x43, txn_op_get_cursors(op));
    txn_op_set_cursors(op, (txn_cursor_t *)0x0);

    ((Transaction *)txn)->free_ops();
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void txnMultipleOpsTest() {
    ham_txn_t *txn;
    txn_opnode_t *node;
    txn_op_t *op1, *op2, *op3;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"world";
    rec.size = 5;

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = txn_opnode_create(m_dbp, &key);
    op1 = txn_opnode_append((Transaction *)txn, node,
        0, TXN_OP_INSERT_DUP, 55, &rec);
    BFC_ASSERT(op1 != 0);
    op2 = txn_opnode_append((Transaction *)txn, node,
        0, TXN_OP_ERASE, 55, &rec);
    BFC_ASSERT(op2 != 0);
    op3 = txn_opnode_append((Transaction *)txn, node,
        0, TXN_OP_NOP, 55, &rec);
    BFC_ASSERT(op3 != 0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void txnInsertConflict1Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));

    /* begin(T1); begin(T2); insert(T1, a); insert(T2, a) -> conflict */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(HAM_TXN_CONFLICT,
          ham_db_insert(m_db, txn2, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

  void txnInsertConflict2Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));

    /* begin(T1); begin(T2); insert(T1, a); insert(T2, a) -> duplicate */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
    BFC_ASSERT_EQUAL(HAM_DUPLICATE_KEY,
          ham_db_insert(m_db, txn2, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

  void txnInsertConflict3Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));

    /* begin(T1); begin(T2); insert(T1, a); commit(T1);
     * insert(T2, a, OW) -> ok */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
    BFC_ASSERT_EQUAL(0,
          ham_db_insert(m_db, txn2, &key, &rec, HAM_OVERWRITE));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

  void txnInsertConflict4Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));

    /* begin(T1); begin(T2); insert(T1, a); commit(T1);
     * insert(T2, a, DUP) -> ok */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
    BFC_ASSERT_EQUAL(0,
          ham_db_insert(m_db, txn2, &key, &rec, HAM_DUPLICATE));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

  void txnInsertConflict5Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));

    /* begin(T1); begin(T2); insert(T1, a); abort(T1);
     * insert(T2, a) */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_abort(txn1, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn2, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

  void txnInsertFind1Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"world";
    rec.size = 5;
    ham_record_t rec2;
    memset(&rec2, 0, sizeof(rec2));

    /* begin(T1); begin(T2); insert(T1, a); commit(T1); find(T2, a) -> ok */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
    BFC_ASSERT_EQUAL(0, ham_db_find(m_db, txn2, &key, &rec2, 0));

    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, memcmp(rec.data, rec2.data, rec2.size));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

  void txnInsertFind2Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"world";
    rec.size = 5;
    ham_record_t rec2;
    memset(&rec2, 0, sizeof(rec2));

    /* begin(T1); begin(T2); insert(T1, a); insert(T2, a) -> conflict */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(HAM_TXN_CONFLICT,
          ham_db_find(m_db, txn2, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

  void txnInsertFind3Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"world";
    rec.size = 5;
    ham_record_t rec2;
    memset(&rec2, 0, sizeof(rec2));

    /* begin(T1); begin(T2); insert(T1, a); commit(T1);
     * commit(T2); find(temp, a) -> ok */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
    BFC_ASSERT_EQUAL(0, ham_db_find(m_db, 0, &key, &rec2, 0));

    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, memcmp(rec.data, rec2.data, rec2.size));
  }

  void txnInsertFind4Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));

    /* begin(T1); begin(T2); insert(T1, a); abort(T1);
     * find(T2, a) -> fail */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_abort(txn1, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          ham_db_find(m_db, txn2, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

  void txnInsertFind5Test(void)
  {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));
    ham_key_t key2;
    memset(&key2, 0, sizeof(key2));
    key2.data = (void *)"world";
    key2.size = 5;

    /* begin(T1); begin(T2); insert(T1, a); commit(T1);
     * find(T2, c) -> fail */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_abort(txn1, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          ham_db_find(m_db, txn2, &key2, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

#if 0
  void txnPartialInsertFindTest(void) {
    ham_txn_t *txn;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data=(void *)"hello";
    key.size=5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.data=(void *)"worldworld";
    rec.size=9;
    rec.partial_offset=1;
    rec.partial_size=2;

    /* insert partial record */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn, &key, &rec, HAM_PARTIAL));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

    /* and read it back */
    ham_record_t rec2;
    memset(&rec2, 0, sizeof(rec2));
    rec2.partial_offset=1;
    rec2.partial_size=2;
    BFC_ASSERT_EQUAL(0, ham_db_find(m_db, txn, &key, &rec2, HAM_PARTIAL));

TODO weiter hier - compare record; must be "\0or\0\0\0\0\0\0\0" (ists
aber nicht)
  }
#endif

  void txnInsertFindErase1Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"world";
    rec.size = 5;
    ham_record_t rec2;
    memset(&rec2, 0, sizeof(rec2));

    /* begin(T1); begin(T2); insert(T1, a); commit(T1); erase(T2, a);
     * find(T2, a) -> fail */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
    BFC_ASSERT_EQUAL(0, ham_db_erase(m_db, txn2, &key, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          ham_db_find(m_db, txn2, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          ham_db_erase(m_db, txn2, &key, 0));
  }

  void txnInsertFindErase2Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"world";
    rec.size = 5;
    ham_record_t rec2;
    memset(&rec2, 0, sizeof(rec2));

    /* begin(T1); begin(T2); insert(T1, a); commit(T1); commit(T2);
     * erase(T3, a) -> ok; find(T2, a) -> fail */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
    BFC_ASSERT_EQUAL(0, ham_db_erase(m_db, txn2, &key, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          ham_db_find(m_db, txn2, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          ham_db_erase(m_db, txn2, &key, 0));
  }

  void txnInsertFindErase3Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"world";
    rec.size = 5;

    /* begin(T1); begin(T2); insert(T1, a); abort(T1); erase(T2, a) -> fail;
     * commit(T2); */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_abort(txn1, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, ham_db_erase(m_db, txn2, &key, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

  void txnInsertFindErase4Test() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key;
    memset(&key, 0, sizeof(key));
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"world";
    rec.size = 5;
    ham_record_t rec2;
    memset(&rec2, 0, sizeof(rec2));

    /* begin(T1); begin(T2); insert(T1, a); erase(T1, a); -> ok;
     * commit(T2); */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn1, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_erase(m_db, txn1, &key, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          ham_db_erase(m_db, txn1, &key, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          ham_db_erase(m_db, txn2, &key, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

};

class HighLevelTxnTest : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  HighLevelTxnTest()
    : hamsterDB_fixture("HighLevelTxnTest") {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(HighLevelTxnTest, noPersistentDatabaseFlagTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, noPersistentEnvironmentFlagTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, cursorStillOpenTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, txnStillOpenTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, clonedCursorStillOpenTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, autoAbortDatabaseTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, autoCommitDatabaseTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, autoAbortEnvironmentTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, autoCommitEnvironmentTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, insertFindCommitTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, insertFindEraseTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, insertFindEraseTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, getKeyCountTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, getKeyCountDupesTest);
    BFC_REGISTER_TEST(HighLevelTxnTest, getKeyCountOverwriteTest);
  }

protected:
  ham_db_t *m_db;
  ham_env_t *m_env;

public:
  virtual void setup() {
  }

  virtual void teardown() {
    if (m_env) {
      BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
      m_env = 0;
    }
  }

  void noPersistentDatabaseFlagTest() {
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
          HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    BFC_ASSERT(HAM_ENABLE_TRANSACTIONS & ((Database *)m_db)->get_rt_flags());
    BFC_ASSERT(HAM_ENABLE_RECOVERY & ((Database *)m_db)->get_rt_flags());
    teardown();

    BFC_ASSERT_EQUAL(0,
        ham_env_open(&m_env, BFC_OPATH(".test"), 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    BFC_ASSERT(!(HAM_ENABLE_TRANSACTIONS & ((Database *)m_db)->get_rt_flags()));
  }

  void noPersistentEnvironmentFlagTest() {
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
          HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT(HAM_ENABLE_TRANSACTIONS & ((Environment *)m_env)->get_flags());
    BFC_ASSERT(HAM_ENABLE_RECOVERY & ((Environment *)m_env)->get_flags());
    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, 0));

    BFC_ASSERT_EQUAL(0, ham_env_open(&m_env, BFC_OPATH(".test"), 0, 0));
    BFC_ASSERT(!(HAM_ENABLE_TRANSACTIONS & ((Environment *)m_env)->get_flags()));
    BFC_ASSERT(!(HAM_ENABLE_RECOVERY & ((Environment *)m_env)->get_flags()));
  }

  void cursorStillOpenTest() {
    ham_txn_t *txn;
    ham_cursor_t *cursor;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, ham_db_get_env(m_db), 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_create(&cursor, m_db, txn, 0));
    BFC_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_commit(txn, 0));
    BFC_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_abort(txn, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void txnStillOpenTest() {
    teardown();
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    ham_txn_t *txn;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn, &key, &rec, 0));
    BFC_ASSERT_EQUAL(HAM_TXN_STILL_OPEN, ham_db_close(m_db, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void clonedCursorStillOpenTest() {
    ham_txn_t *txn;
    ham_cursor_t *cursor, *clone;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
            
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, ham_db_get_env(m_db), 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_create(&cursor, m_db, txn, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_clone(cursor, &clone));
    BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    BFC_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_commit(txn, 0));
    BFC_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_abort(txn, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));
    BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
  }

  void autoAbortDatabaseTest()
  {
    ham_txn_t *txn;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, ham_db_get_env(m_db), 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_find(m_db, txn, &key, &rec, 0));
    teardown();

    BFC_ASSERT_EQUAL(0,
        ham_env_open(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(m_env, &m_db, 1, 0, 0));

    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
            ham_db_find(m_db, 0, &key, &rec, 0));
  }

  void autoCommitDatabaseTest() {
    ham_txn_t *txn;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, ham_db_get_env(m_db), 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_find(m_db, txn, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_close(m_env, HAM_AUTO_CLEANUP | HAM_TXN_AUTO_COMMIT));

    BFC_ASSERT_EQUAL(0,
        ham_env_open(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(m_env, &m_db, 1, 0, 0));

    BFC_ASSERT_EQUAL(0,
        ham_db_find(m_db, 0, &key, &rec, 0));
  }

  void autoAbortEnvironmentTest() {
    ham_txn_t *txn;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_find(m_db, txn, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, 0));

    BFC_ASSERT_EQUAL(0,
        ham_env_open(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
        ham_db_find(m_db, 0, &key, &rec, 0));
  }

  void autoCommitEnvironmentTest() {
    ham_txn_t *txn;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_find(m_db, txn, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_close(m_env, HAM_AUTO_CLEANUP | HAM_TXN_AUTO_COMMIT));

    BFC_ASSERT_EQUAL(0,
        ham_env_open(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_db_find(m_db, 0, &key, &rec, 0));
  }

  void insertFindCommitTest() {
    ham_txn_t *txn;
    ham_key_t key;
    ham_record_t rec, rec2;
    ham_u8_t buffer[64];
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    ::memset(&rec2, 0, sizeof(rec));
    rec.data = &buffer[0];
    rec.size = sizeof(buffer);

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
          HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_find(m_db, txn, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(HAM_TXN_CONFLICT, ham_db_find(m_db, 0, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    BFC_ASSERT_EQUAL(0, ham_db_find(m_db, 0, &key, &rec2, 0));
  }

  void insertFindEraseTest()
  {
    ham_txn_t *txn;
    ham_key_t key;
    ham_record_t rec;
    ham_u8_t buffer[64];
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.data = &buffer[0];
    rec.size = sizeof(buffer);

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
          HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_find(m_db, txn, &key, &rec, 0));
    BFC_ASSERT_EQUAL(HAM_TXN_CONFLICT, ham_db_erase(m_db, 0, &key, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    BFC_ASSERT_EQUAL(0, ham_db_erase(m_db, 0, &key, 0));
  }

  ham_status_t insert(ham_txn_t *txn, const char *keydata,
          const char *recorddata, int flags) {
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    key.data = (void *)keydata;
    key.size = strlen(keydata) + 1;
    rec.data = (void *)recorddata;
    rec.size = strlen(recorddata) + 1;

    return (ham_db_insert(m_db, txn, &key, &rec, flags));
  }

  ham_status_t find(ham_txn_t *txn, const char *keydata,
          const char *recorddata) {
    ham_status_t st;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    key.data = (void *)keydata;
    key.size = strlen(keydata) + 1;

    st=ham_db_find(m_db, txn, &key, &rec, 0);
    if (st)
      return (st);
    BFC_ASSERT_EQUAL(0, strcmp(recorddata, (char *)rec.data));
    BFC_ASSERT_EQUAL(rec.size, strlen(recorddata) + 1);
    return (0);
  }

  void getKeyCountTest() {
    ham_txn_t *txn;
    ham_u64_t count;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
          HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    /* without txn */
    BFC_ASSERT_EQUAL(0, insert(0, "key1", "rec1", 0));
    BFC_ASSERT_EQUAL(0, find(0, "key1", "rec1"));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(m_db, 0, 0, &count));
    BFC_ASSERT_EQUAL(1ull, count);

    /* in an active txn */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, ham_db_get_env(m_db), 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(m_db, txn, 0, &count));
    BFC_ASSERT_EQUAL(1ull, count);
    BFC_ASSERT_EQUAL(0, insert(txn, "key2", "rec2", 0));
    BFC_ASSERT_EQUAL(HAM_TXN_CONFLICT, find(0, "key2", "rec2"));
    BFC_ASSERT_EQUAL(0, find(txn, "key2", "rec2"));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(m_db, txn, 0, &count));
    BFC_ASSERT_EQUAL(2ull, count);
    BFC_ASSERT_EQUAL(0, insert(txn, "key2", "rec2", HAM_OVERWRITE));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(m_db, txn, 0, &count));
    BFC_ASSERT_EQUAL(2ull, count);
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    BFC_ASSERT_EQUAL(0, find(0, "key2", "rec2"));

    /* after commit */
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(m_db, 0, 0, &count));
    BFC_ASSERT_EQUAL(2ull, count);

    /* in temp. txn */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, ham_db_get_env(m_db), 0, 0, 0));
    BFC_ASSERT_EQUAL(0, insert(txn, "key3", "rec1", 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(m_db, txn, 0, &count));
    BFC_ASSERT_EQUAL(3ull, count);
    BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));

    /* after abort */
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(m_db, 0, 0, &count));
    BFC_ASSERT_EQUAL(2ull, count);
  }

  void getKeyCountDupesTest() {
    ham_txn_t *txn;
    ham_u64_t count;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
          HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, HAM_ENABLE_DUPLICATES, 0));

    /* without txn */
    BFC_ASSERT_EQUAL(0, insert(0, "key1", "rec1", 0));
    BFC_ASSERT_EQUAL(0, insert(0, "key2", "rec1", 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(m_db, 0, 0, &count));
    BFC_ASSERT_EQUAL(2ull, count);

    /* in an active txn */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, ham_db_get_env(m_db), 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(m_db, txn, 0, &count));
    BFC_ASSERT_EQUAL(2ull, count);
    BFC_ASSERT_EQUAL(0, insert(txn, "key3", "rec3", 0));
    BFC_ASSERT_EQUAL(0, insert(txn, "key3", "rec4", HAM_DUPLICATE));
    BFC_ASSERT_EQUAL(0,
          ham_db_get_key_count(m_db, txn, 0, &count));
    BFC_ASSERT_EQUAL(4ull, count);
    BFC_ASSERT_EQUAL(0,
          ham_db_get_key_count(m_db, txn, HAM_SKIP_DUPLICATES, &count));
    BFC_ASSERT_EQUAL(3ull, count);
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

    /* after commit */
    BFC_ASSERT_EQUAL(0,
          ham_db_get_key_count(m_db, txn, 0, &count));
    BFC_ASSERT_EQUAL(4ull, count);
    BFC_ASSERT_EQUAL(0,
          ham_db_get_key_count(m_db, txn, HAM_SKIP_DUPLICATES, &count));
    BFC_ASSERT_EQUAL(3ull, count);
  }

  void getKeyCountOverwriteTest() {
    ham_txn_t *txn;
    ham_u64_t count;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
          HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, HAM_ENABLE_DUPLICATES, 0));

    /* without txn */
    BFC_ASSERT_EQUAL(0, insert(0, "key1", "rec1", 0));
    BFC_ASSERT_EQUAL(0, insert(0, "key2", "rec1", 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(m_db, 0, 0, &count));
    BFC_ASSERT_EQUAL(2ull, count);

    /* in an active txn */
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, ham_db_get_env(m_db), 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(m_db, txn, 0, &count));
    BFC_ASSERT_EQUAL(2ull, count);
    BFC_ASSERT_EQUAL(0, insert(txn, "key2", "rec4", HAM_OVERWRITE));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(m_db, txn, 0, &count));
    BFC_ASSERT_EQUAL(2ull, count);
    BFC_ASSERT_EQUAL(0, insert(txn, "key3", "rec3", 0));
    BFC_ASSERT_EQUAL(0, insert(txn, "key3", "rec4", HAM_OVERWRITE));
    BFC_ASSERT_EQUAL(0,
          ham_db_get_key_count(m_db, txn, 0, &count));
    BFC_ASSERT_EQUAL(3ull, count);
    BFC_ASSERT_EQUAL(0,
          ham_db_get_key_count(m_db, txn, HAM_SKIP_DUPLICATES, &count));
    BFC_ASSERT_EQUAL(3ull, count);
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

    /* after commit */
    BFC_ASSERT_EQUAL(0,
          ham_db_get_key_count(m_db, txn, 0, &count));
    BFC_ASSERT_EQUAL(3ull, count);
    BFC_ASSERT_EQUAL(0,
          ham_db_get_key_count(m_db, txn, HAM_SKIP_DUPLICATES, &count));
    BFC_ASSERT_EQUAL(3ull, count);
  }
};

BFC_REGISTER_FIXTURE(TxnTest);
BFC_REGISTER_FIXTURE(HighLevelTxnTest);

