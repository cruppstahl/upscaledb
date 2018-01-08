/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */
 

#include "3rdparty/catch/catch.hpp"

#include <ups/upscaledb.h>

#include "4db/db_local.h"
#include "4env/env_local.h"
#include "4txn/txn_local.h"

#include "os.hpp"
#include "fixture.hpp"

namespace upscaledb {

struct TxnFixture : BaseFixture {

  TxnFixture() {
    require_create(UPS_ENABLE_TRANSACTIONS);
  }

  ~TxnFixture() {
    close();
  }

  void beginCommitTest() {
    TxnProxy txnp(env, nullptr, true);
    // will commit on exit
  }

  void multipleBeginCommitTest() {
    TxnProxy txnp1(env);
    TxnProxy txnp2(env);
    TxnProxy txnp3(env);

    txnp1.require_next(txnp2.txn);
    txnp2.require_next(txnp3.txn);
    txnp3.require_next(nullptr);

    // have to commit the txns in the same order as they were created,
    // otherwise env_flush_committed_txns() will not flush the oldest
    // transaction
    txnp1.commit();

    txnp2.require_next(txnp3.txn);
    txnp3.require_next(nullptr);

    txnp2.commit();

    txnp3.require_next(nullptr);

    txnp3.commit();
  }

  void beginAbortTest() {
    TxnProxy txnp(env);
    // will abort on exit
  }

  void txnMultipleTreesTest() {
    ups_db_t *db2, *db3;

    REQUIRE(0 == ups_env_create_db(env, &db2, 14, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db3, 15, 0, 0));

    TxnProxy txnp(env);
    TxnIndex *tree1 = ldb(db)->txn_index.get();
    TxnIndex *tree2 = ldb(db2)->txn_index.get();
    TxnIndex *tree3 = ldb(db3)->txn_index.get();

    REQUIRE(tree1 != nullptr);
    REQUIRE(tree2 != nullptr);
    REQUIRE(tree3 != nullptr);

    txnp.commit();
  }

  void txnNodeCreatedOnceTest() {
    ups_key_t key1 = ups_make_key((void *)"hello", 5);
    ups_key_t key2 = ups_make_key((void *)"world", 5);

    TxnProxy txnp(env);

    bool node_created;
    TxnNode *node1 = ldb()->txn_index->store(&key1, &node_created);
    TxnNode *node2 = ldb()->txn_index->get(&key1, 0);
    REQUIRE(node1 == node2);
    node2 = ldb()->txn_index->get(&key2, 0);
    REQUIRE(node2 == nullptr);
    node2 = ldb()->txn_index->store(&key2, &node_created);
    REQUIRE(node1 != node2);

    // clean up
    ldb()->txn_index->remove(node1);
    delete node1;
    ldb()->txn_index->remove(node2);
    delete node2;
  }

  void txnMultipleNodesTest() {
    ups_key_t key1 = ups_make_key((void *)"1111", 5);
    ups_key_t key2 = ups_make_key((void *)"2222", 5);
    ups_key_t key3 = ups_make_key((void *)"3333", 5);

    TxnProxy txnp(env);

    bool node_created;
    TxnNode *node1 = ldb()->txn_index->store(&key1, &node_created);
    TxnNode *node2 = ldb()->txn_index->store(&key2, &node_created);
    TxnNode *node3 = ldb()->txn_index->store(&key3, &node_created);

    // clean up
    ldb()->txn_index->remove(node1);
    delete node1;
    ldb()->txn_index->remove(node2);
    delete node2;
    ldb()->txn_index->remove(node3);
    delete node3;
  }

  void txnMultipleOpsTest() {
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = ups_make_record((void *)"world", 5);
    TxnProxy txnp(env);

    bool node_created;
    TxnNode *node = ldb()->txn_index->store(&key, &node_created);
    TxnOperation *op1 = node->append(txnp.ltxn(), 0,
                    TxnOperation::kInsertDuplicate, 55, &key, &rec);
    REQUIRE(op1 != nullptr);
    TxnOperation *op2 = node->append(txnp.ltxn(), 0,
                    TxnOperation::kErase, 56, &key, &rec);
    REQUIRE(op2 != nullptr);
    TxnOperation *op3 = node->append(txnp.ltxn(), 0,
                TxnOperation::kNop, 57, &key, &rec);
    REQUIRE(op3 != nullptr);
  }

  void txnInsertConflict1Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = {0};

    // begin(T1); begin(T2); insert(T1, a); insert(T2, a) -> conflict 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(UPS_TXN_CONFLICT == ups_db_insert(db, txn2, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn1, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void txnInsertConflict2Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = {0};

    // begin(T1); begin(T2); insert(T1, a); insert(T2, a) -> duplicate 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn1, 0));
    REQUIRE(UPS_DUPLICATE_KEY == ups_db_insert(db, txn2, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void txnInsertConflict3Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = {0};

    // begin(T1); begin(T2); insert(T1, a); commit(T1);
    // insert(T2, a, OW) -> ok 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn1, 0));
    REQUIRE(0 == ups_db_insert(db, txn2, &key, &rec, UPS_OVERWRITE));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void txnInsertConflict4Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = {0};

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, 0, UPS_ENABLE_DUPLICATES, 0);

    // begin(T1); begin(T2); insert(T1, a); commit(T1);
    // insert(T2, a, DUP) -> ok 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn1, 0));
    REQUIRE(0 == ups_db_insert(db, txn2, &key, &rec, UPS_DUPLICATE));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void txnInsertConflict5Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = {0};

    // begin(T1); begin(T2); insert(T1, a); abort(T1);
    // insert(T2, a) 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(0 == ups_txn_abort(txn1, 0));
    REQUIRE(0 == ups_db_insert(db, txn2, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void txnInsertFind1Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = ups_make_record((void *)"world", 5);
    ups_record_t rec2 = {0};

    // begin(T1); begin(T2); insert(T1, a); commit(T1); find(T2, a) -> ok 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn1, 0));
    REQUIRE(0 == ups_db_find(db, txn2, &key, &rec2, 0));

    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::memcmp(rec.data, rec2.data, rec2.size));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void txnInsertFind2Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = ups_make_record((void *)"world", 5);
    ups_record_t rec2 = {0};

    // begin(T1); begin(T2); insert(T1, a); insert(T2, a) -> conflict 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(UPS_TXN_CONFLICT == ups_db_find(db, txn2, &key, &rec2, 0));
    REQUIRE(0 == ups_txn_commit(txn1, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void txnInsertFind3Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = ups_make_record((void *)"world", 5);
    ups_record_t rec2 = {0};

    // begin(T1); begin(T2); insert(T1, a); commit(T1);
    // commit(T2); find(temp, a) -> ok 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn1, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));

    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::memcmp(rec.data, rec2.data, rec2.size));
  }

  void txnInsertFind4Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = {0};

    // begin(T1); begin(T2); insert(T1, a); abort(T1);
    // find(T2, a) -> fail 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(0 == ups_txn_abort(txn1, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, txn2, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void txnInsertFind5Test(void)
  {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_key_t key2 = ups_make_key((void *)"world", 5);
    ups_record_t rec = {0};

    // begin(T1); begin(T2); insert(T1, a); commit(T1);
    // find(T2, c) -> fail 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(0 == ups_txn_abort(txn1, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, txn2, &key2, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void txnInsertFindErase1Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = ups_make_record((void *)"world", 5);
    ups_record_t rec2 = {0};

    // begin(T1); begin(T2); insert(T1, a); commit(T1); erase(T2, a);
    // find(T2, a) -> fail 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn1, 0));
    REQUIRE(0 == ups_db_erase(db, txn2, &key, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, txn2, &key, &rec2, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_erase(db, 0, &key, 0));
  }

  void txnInsertFindErase2Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = ups_make_record((void *)"world", 5);
    ups_record_t rec2 = {0};

    // begin(T1); begin(T2); insert(T1, a); commit(T1); commit(T2);
    // erase(T3, a) -> ok; find(T2, a) -> fail 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn1, 0));
    REQUIRE(0 == ups_db_erase(db, txn2, &key, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, txn2, &key, &rec2, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_erase(db, 0, &key, 0));
  }

  void txnInsertFindErase3Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = ups_make_record((void *)"world", 5);

    // begin(T1); begin(T2); insert(T1, a); abort(T1); erase(T2, a) -> fail;
    // commit(T2); 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(0 == ups_txn_abort(txn1, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_erase(db, txn2, &key, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void txnInsertFindErase4Test() {
    ups_txn_t *txn1, *txn2;
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_record_t rec = ups_make_record((void *)"world", 5);
    ups_record_t rec2 = {0};

    // begin(T1); begin(T2); insert(T1, a); erase(T1, a); -> ok;
    // commit(T2); 
    REQUIRE(0 == ups_txn_begin(&txn1, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn1, &key, &rec, 0));
    REQUIRE(0 == ups_db_erase(db, txn1, &key, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_erase(db, txn1, &key, 0));
    REQUIRE(0 == ups_txn_commit(txn1, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_erase(db, txn2, &key, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void issue105Test() {
    const int item_count = 50;
    for (int i = 0; i < item_count; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      ups_record_t rec = {0};
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    }

    uint64_t count = 0;
    REQUIRE(0 == ups_db_count(db, 0, 0, &count));
    REQUIRE(count == item_count);

    for (int i = 0; i < item_count / 2; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      REQUIRE(0 == ups_db_erase(db, 0, &key, 0));

      REQUIRE(0 == ups_db_count(db, 0, 0, &count));
      REQUIRE(count == item_count - i - 1);
    }

    REQUIRE(0 == ups_db_count(db, 0, 0, &count));
    REQUIRE(count == item_count / 2);

#if 0
    //for (int i = 0; i < item_count / 2; i++) {
    for (int i = 23; i < item_count / 2; i++) {
        std::cout << "searching for " << i << std::endl;
      ups_key_t key = ups_make_key(&i, sizeof(i));
      ups_record_t record = {0};

      ups_status_t st = ups_db_find(db, 0, &key, &record, UPS_FIND_GEQ_MATCH);
      if (st == UPS_SUCCESS) {// && *reinterpret_cast<int*>(key.data) == i) {
        std::cout << "Found deleted item: " << i << std::endl;
      }
    }
#endif
  }
};

TEST_CASE("Txn/beginCommitTest", "")
{
  TxnFixture f;
  f.beginCommitTest();
}

TEST_CASE("Txn/multipleBeginCommitTest", "")
{
  TxnFixture f;
  f.multipleBeginCommitTest();
}

TEST_CASE("Txn/beginAbortTest", "")
{
  TxnFixture f;
  f.beginAbortTest();
}

TEST_CASE("Txn/txnMultipleTreesTest", "")
{
  TxnFixture f;
  f.txnMultipleTreesTest();
}

TEST_CASE("Txn/txnNodeCreatedOnceTest", "")
{
  TxnFixture f;
  f.txnNodeCreatedOnceTest();
}

TEST_CASE("Txn/txnMultipleNodesTest", "")
{
  TxnFixture f;
  f.txnMultipleNodesTest();
}

TEST_CASE("Txn/txnMultipleOpsTest", "")
{
  TxnFixture f;
  f.txnMultipleOpsTest();
}

TEST_CASE("Txn/txnInsertConflict1Test", "")
{
  TxnFixture f;
  f.txnInsertConflict1Test();
}

TEST_CASE("Txn/txnInsertConflict2Test", "")
{
  TxnFixture f;
  f.txnInsertConflict2Test();
}

TEST_CASE("Txn/txnInsertConflict3Test", "")
{
  TxnFixture f;
  f.txnInsertConflict3Test();
}

TEST_CASE("Txn/txnInsertConflict4Test", "")
{
  TxnFixture f;
  f.txnInsertConflict4Test();
}

TEST_CASE("Txn/txnInsertConflict5Test", "")
{
  TxnFixture f;
  f.txnInsertConflict5Test();
}

TEST_CASE("Txn/txnInsertFind1Test", "")
{
  TxnFixture f;
  f.txnInsertFind1Test();
}

TEST_CASE("Txn/txnInsertFind2Test", "")
{
  TxnFixture f;
  f.txnInsertFind2Test();
}

TEST_CASE("Txn/txnInsertFind3Test", "")
{
  TxnFixture f;
  f.txnInsertFind3Test();
}

TEST_CASE("Txn/txnInsertFind4Test", "")
{
  TxnFixture f;
  f.txnInsertFind4Test();
}

TEST_CASE("Txn/txnInsertFind5Test", "")
{
  TxnFixture f;
  f.txnInsertFind5Test();
}

TEST_CASE("Txn/txnInsertFindErase1Test", "")
{
  TxnFixture f;
  f.txnInsertFindErase1Test();
}

TEST_CASE("Txn/txnInsertFindErase2Test", "")
{
  TxnFixture f;
  f.txnInsertFindErase2Test();
}

TEST_CASE("Txn/txnInsertFindErase3Test", "")
{
  TxnFixture f;
  f.txnInsertFindErase3Test();
}

TEST_CASE("Txn/txnInsertFindErase4Test", "")
{
  TxnFixture f;
  f.txnInsertFindErase4Test();
}


struct HighLevelTxnFixture : BaseFixture {

  ~HighLevelTxnFixture() {
    close();
  }

  void noPersistentDatabaseFlagTest() {
    require_create(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(ISSET(ldb()->flags(), UPS_ENABLE_TRANSACTIONS));
    close();
    require_open(0);
    REQUIRE(NOTSET(ldb()->flags(), UPS_ENABLE_TRANSACTIONS));
  }

  void noPersistentEnvironmentFlagTest() {
    require_create(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(ISSET(lenv()->flags(), UPS_ENABLE_TRANSACTIONS));
    close();
    require_open();
    REQUIRE(NOTSET(lenv()->flags(), UPS_ENABLE_TRANSACTIONS));
  }

  void cursorStillOpenTest() {
    ups_txn_t *txn;
    ups_cursor_t *cursor;

    require_create(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, txn, 0));
    REQUIRE(UPS_CURSOR_STILL_OPEN == ups_txn_commit(txn, 0));
    REQUIRE(UPS_CURSOR_STILL_OPEN == ups_txn_abort(txn, 0));
    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void txnStillOpenTest() {
    require_create(UPS_ENABLE_TRANSACTIONS);

    ups_txn_t *txn;
    ups_key_t key = {0};
    ups_record_t rec = {0};

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
    REQUIRE(UPS_TXN_STILL_OPEN == ups_db_close(db, 0));
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void clonedCursorStillOpenTest() {
    ups_txn_t *txn;
    ups_cursor_t *cursor, *clone;

    require_create(UPS_ENABLE_TRANSACTIONS);
            
    REQUIRE(0 == ups_txn_begin(&txn, ups_db_get_env(db), 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, txn, 0));
    REQUIRE(0 == ups_cursor_clone(cursor, &clone));
    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(UPS_CURSOR_STILL_OPEN == ups_txn_commit(txn, 0));
    REQUIRE(UPS_CURSOR_STILL_OPEN == ups_txn_abort(txn, 0));
    REQUIRE(0 == ups_cursor_close(clone));
    REQUIRE(0 == ups_txn_abort(txn, 0));
  }

  void autoAbortDatabaseTest() {
    ups_txn_t *txn;
    ups_key_t key = {0};
    ups_record_t rec = {0};

    require_create(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(0 == ups_txn_begin(&txn, ups_db_get_env(db), 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(db, txn, &key, &rec, 0));
    close();
    require_open(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, 0, &key, &rec, 0));
  }

  void autoCommitDatabaseTest() {
    ups_txn_t *txn;
    ups_key_t key = {0};
    ups_record_t rec = {0};

    require_create(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(0 == ups_txn_begin(&txn, ups_db_get_env(db), 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP | UPS_TXN_AUTO_COMMIT));

    require_open(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
  }

  void autoAbortEnvironmentTest() {
    ups_txn_t *txn;
    ups_key_t key = {0};
    ups_record_t rec = {0};

    require_create(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(db, txn, &key, &rec, 0));

    close();
    require_open(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, 0, &key, &rec, 0));
  }

  void autoCommitEnvironmentTest() {
    ups_txn_t *txn;
    ups_key_t key = {0};
    ups_record_t rec = {0};

    require_create(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP | UPS_TXN_AUTO_COMMIT));

    require_open(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
  }

  void insertFindCommitTest() {
    ups_txn_t *txn;
    uint8_t buffer[64] = {0};
    ups_key_t key = {0};
    ups_record_t rec = ups_make_record(buffer, sizeof(buffer));
    ups_record_t rec2 = {0};

    require_create(UPS_ENABLE_TRANSACTIONS);
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(db, txn, &key, &rec2, 0));
    REQUIRE(UPS_TXN_CONFLICT == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(0 == ups_txn_commit(txn, 0));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
  }

  void insertFindEraseTest() {
    ups_txn_t *txn;
    uint8_t buffer[64] = {0};
    ups_key_t key = {0};
    ups_record_t rec = ups_make_record(buffer, sizeof(buffer));

    require_create(UPS_ENABLE_TRANSACTIONS);

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(db, txn, &key, &rec, 0));
    REQUIRE(UPS_TXN_CONFLICT == ups_db_erase(db, 0, &key, 0));
    REQUIRE(0 == ups_txn_commit(txn, 0));
    REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
  }

  ups_status_t insert(ups_txn_t *txn, const char *keydata,
                  const char *recorddata, int flags) {
    ups_key_t key = ups_make_key((void *)keydata,
                    (uint16_t)(::strlen(keydata) + 1));
    ups_record_t rec = ups_make_record((void *)recorddata,
                    (uint32_t)(::strlen(recorddata) + 1));

    return ups_db_insert(db, txn, &key, &rec, flags);
  }

  ups_status_t find(ups_txn_t *txn, const char *keydata,
                  const char *recorddata) {
    ups_key_t key = ups_make_key((void *)keydata,
                    (uint16_t)(::strlen(keydata) + 1));
    ups_record_t rec = {0};
    ups_status_t st = ups_db_find(db, txn, &key, &rec, 0);
    if (st)
      return st;
    REQUIRE(0 == ::strcmp(recorddata, (char *)rec.data));
    REQUIRE(rec.size == (uint32_t)::strlen(recorddata) + 1);
    return 0;
  }

  void getKeyCountTest() {
    ups_txn_t *txn;
    uint64_t count;

    require_create(UPS_ENABLE_TRANSACTIONS);

    // without txn 
    REQUIRE(0 == insert(0, "key1", "rec1", 0));
    REQUIRE(0 == find(0, "key1", "rec1"));
    REQUIRE(0 == ups_db_count(db, 0, 0, &count));
    REQUIRE(1ull == count);

    // in an active txn 
    REQUIRE(0 == ups_txn_begin(&txn, ups_db_get_env(db), 0, 0, 0));
    REQUIRE(0 == ups_db_count(db, txn, 0, &count));
    REQUIRE(1ull == count);
    REQUIRE(0 == insert(txn, "key2", "rec2", 0));
    REQUIRE(UPS_TXN_CONFLICT == find(0, "key2", "rec2"));
    REQUIRE(0 == find(txn, "key2", "rec2"));
    REQUIRE(0 == ups_db_count(db, txn, 0, &count));
    REQUIRE(2ull == count);
    REQUIRE(0 == insert(txn, "key2", "rec2", UPS_OVERWRITE));
    REQUIRE(0 == ups_db_count(db, txn, 0, &count));
    REQUIRE(2ull == count);
    REQUIRE(0 == ups_txn_commit(txn, 0));
    REQUIRE(0 == find(0, "key2", "rec2"));

    // after commit 
    REQUIRE(0 == ups_db_count(db, 0, 0, &count));
    REQUIRE(2ull == count);

    // in temp. txn 
    REQUIRE(0 == ups_txn_begin(&txn, ups_db_get_env(db), 0, 0, 0));
    REQUIRE(0 == insert(txn, "key3", "rec1", 0));
    REQUIRE(0 == ups_db_count(db, txn, 0, &count));
    REQUIRE(3ull == count);
    REQUIRE(0 == ups_txn_abort(txn, 0));

    // after abort 
    REQUIRE(0 == ups_db_count(db, 0, 0, &count));
    REQUIRE(2ull == count);
  }

  void getKeyCountDupesTest() {
    ups_txn_t *txn;
    uint64_t count;

    require_create(UPS_ENABLE_TRANSACTIONS, 0, UPS_ENABLE_DUPLICATES, 0);

    // without txn 
    REQUIRE(0 == insert(0, "key1", "rec1", 0));
    REQUIRE(0 == insert(0, "key2", "rec1", 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &count));
    REQUIRE(2ull == count);

    // in an active txn 
    REQUIRE(0 == ups_txn_begin(&txn, ups_db_get_env(db), 0, 0, 0));
    REQUIRE(0 == ups_db_count(db, txn, 0, &count));
    REQUIRE(2ull == count);
    REQUIRE(0 == insert(txn, "key3", "rec3", 0));
    REQUIRE(0 == insert(txn, "key3", "rec4", UPS_DUPLICATE));
    REQUIRE(0 == ups_db_count(db, txn, 0, &count));
    REQUIRE(4ull == count);
    REQUIRE(0 == ups_db_count(db, txn, UPS_SKIP_DUPLICATES, &count));
    REQUIRE(3ull == count);
    REQUIRE(0 == ups_txn_commit(txn, 0));

    // after commit 
    REQUIRE(0 == ups_db_count(db, 0, 0, &count));
    REQUIRE(4ull == count);
    REQUIRE(0 == ups_db_count(db, 0, UPS_SKIP_DUPLICATES, &count));
    REQUIRE(3ull == count);
  }

  void getKeyCountOverwriteTest() {
    ups_txn_t *txn;
    uint64_t count;

    require_create(UPS_ENABLE_TRANSACTIONS);

    // without txn 
    REQUIRE(0 == insert(0, "key1", "rec1", 0));
    REQUIRE(0 == insert(0, "key2", "rec1", 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &count));
    REQUIRE(2ull == count);

    // in an active txn 
    REQUIRE(0 == ups_txn_begin(&txn, ups_db_get_env(db), 0, 0, 0));
    REQUIRE(0 == ups_db_count(db, txn, 0, &count));
    REQUIRE(2ull == count);
    REQUIRE(0 == insert(txn, "key2", "rec4", UPS_OVERWRITE));
    REQUIRE(0 == ups_db_count(db, txn, 0, &count));
    REQUIRE(2ull == count);
    REQUIRE(0 == insert(txn, "key3", "rec3", 0));
    REQUIRE(0 == insert(txn, "key3", "rec4", UPS_OVERWRITE));
    REQUIRE(0 ==
          ups_db_count(db, txn, 0, &count));
    REQUIRE(3ull == count);
    REQUIRE(0 ==
          ups_db_count(db, txn, UPS_SKIP_DUPLICATES, &count));
    REQUIRE(3ull == count);
    REQUIRE(0 == ups_txn_commit(txn, 0));

    // after commit 
    REQUIRE(0 == ups_db_count(db, 0, 0, &count));
    REQUIRE(3ull == count);
    REQUIRE(0 == ups_db_count(db, 0, UPS_SKIP_DUPLICATES, &count));
    REQUIRE(3ull == count);
  }

  void insertTxnsWithDelay(int loop) {
    ups_txn_t *txn;

    require_create(UPS_ENABLE_TRANSACTIONS);

    for (int i = 0; i < loop; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      ups_record_t rec = ups_make_record(&i, sizeof(i));
      REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
      REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
      REQUIRE(0 == ups_txn_commit(txn, 0));
    }

    // reopen the environment
    close();
    require_open(UPS_ENABLE_TRANSACTIONS);

    // and check that the values exist
    for (int i = 0; i < loop; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      ups_record_t rec = {0};
      REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
      REQUIRE(rec.size == sizeof(i));
      REQUIRE(*(int *)rec.data == i);
    }

    close();
  }
};

TEST_CASE("Txn/high/noPersistentDatabaseFlagTest", "")
{
  HighLevelTxnFixture f;
  f.noPersistentDatabaseFlagTest();
}

TEST_CASE("Txn/high/noPersistentEnvironmentFlagTest", "")
{
  HighLevelTxnFixture f;
  f.noPersistentEnvironmentFlagTest();
}

TEST_CASE("Txn/high/cursorStillOpenTest", "")
{
  HighLevelTxnFixture f;
  f.cursorStillOpenTest();
}

TEST_CASE("Txn/high/txnStillOpenTest", "")
{
  HighLevelTxnFixture f;
  f.txnStillOpenTest();
}

TEST_CASE("Txn/high/clonedCursorStillOpenTest", "")
{
  HighLevelTxnFixture f;
  f.clonedCursorStillOpenTest();
}

TEST_CASE("Txn/high/autoAbortDatabaseTest", "")
{
  HighLevelTxnFixture f;
  f.autoAbortDatabaseTest();
}

TEST_CASE("Txn/high/autoCommitDatabaseTest", "")
{
  HighLevelTxnFixture f;
  f.autoCommitDatabaseTest();
}

TEST_CASE("Txn/high/autoAbortEnvironmentTest", "")
{
  HighLevelTxnFixture f;
  f.autoAbortEnvironmentTest();
}

TEST_CASE("Txn/high/autoCommitEnvironmentTest", "")
{
  HighLevelTxnFixture f;
  f.autoCommitEnvironmentTest();
}

TEST_CASE("Txn/high/insertFindCommitTest", "")
{
  HighLevelTxnFixture f;
  f.insertFindCommitTest();
}

TEST_CASE("Txn/high/insertFindEraseTest", "")
{
  HighLevelTxnFixture f;
  f.insertFindEraseTest();
}

TEST_CASE("Txn/high/getKeyCountTest", "")
{
  HighLevelTxnFixture f;
  f.getKeyCountTest();
}

TEST_CASE("Txn/high/getKeyCountDupesTest", "")
{
  HighLevelTxnFixture f;
  f.getKeyCountDupesTest();
}

TEST_CASE("Txn/high/getKeyCountOverwriteTest", "")
{
  HighLevelTxnFixture f;
  f.getKeyCountOverwriteTest();
}

TEST_CASE("Txn/high/insertTxnsWithDelay", "")
{
  HighLevelTxnFixture f;
  for (int i = 1; i < 30; i++)
    f.insertTxnsWithDelay(i);
}

struct InMemoryTxnFixture : BaseFixture {
  InMemoryTxnFixture() {
    require_create(UPS_IN_MEMORY | UPS_ENABLE_TRANSACTIONS, 0,
                    UPS_ENABLE_DUPLICATE_KEYS, 0);
  }

  ~InMemoryTxnFixture() {
    close();
  }

  void createCloseTest() {
    // nop
  }

  void insertTest() {
    ups_txn_t *txn;
    ups_key_t key = {};
    ups_record_t rec = {};

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_txn_abort(txn, 0));
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void eraseTest() {
    ups_txn_t *txn;
    ups_key_t key = {};
    ups_record_t rec = {};

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_db_erase(db, txn, &key, 0));
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void findTest() {
    ups_txn_t *txn;
    ups_key_t key = {};
    ups_record_t rec = {};

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_db_erase(db, txn, &key, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, txn, &key, &rec, 0));
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void cursorInsertTest() {
    ups_txn_t *txn;
    ups_cursor_t *cursor;
    ups_key_t key = {};
    ups_record_t rec = {};

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, txn, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void cursorEraseTest() {
    ups_txn_t *txn;
    ups_cursor_t *cursor;
    ups_key_t key = {};
    ups_record_t rec = {};

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, txn, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(cursor, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void cursorFindTest() {
    ups_txn_t *txn;
    ups_cursor_t *cursor;
    ups_key_t key = {};
    ups_record_t rec = {};

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, txn, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_txn_commit(txn, 0));

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, txn, 0));
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void cursorGetDuplicateCountTest() {
    ups_txn_t *txn;
    ups_cursor_t *cursor;
    ups_key_t key = {};
    ups_record_t rec = {};

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, txn, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, UPS_DUPLICATE));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, UPS_DUPLICATE));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, UPS_DUPLICATE));
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));

    uint64_t keycount;
    REQUIRE(0 == ups_db_count(db, txn, 0, &keycount));
    REQUIRE(3ull == keycount);

    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void cursorGetRecordSizeTest() {
    ups_txn_t *txn;
    ups_cursor_t *cursor;
    ups_key_t key = {};
    ups_record_t rec = ups_make_record((void *)"12345", 6);

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, txn, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));

    uint32_t rec_size;
    REQUIRE(0 == ups_cursor_get_record_size(cursor, &rec_size));
    REQUIRE(6ull == rec_size);

    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void cursorOverwriteTest() {
    ups_txn_t *txn;
    ups_cursor_t *cursor;
    ups_key_t key = {};
    ups_record_t rec = ups_make_record((void *)"12345", 6);
    ups_record_t rec2 = ups_make_record((void *)"1234567890", 11);

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, txn, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_overwrite(cursor, &rec2, 0));
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, 0));

    REQUIRE(11u == rec.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, "1234567890"));

    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }
};

TEST_CASE("Txn/inmem/createCloseTest", "")
{
  InMemoryTxnFixture f;
  f.createCloseTest();
}

TEST_CASE("Txn/inmem/insertTest", "")
{
  InMemoryTxnFixture f;
  f.insertTest();
}

TEST_CASE("Txn/inmem/eraseTest", "")
{
  InMemoryTxnFixture f;
  f.eraseTest();
}

TEST_CASE("Txn/inmem/findTest", "")
{
  InMemoryTxnFixture f;
  f.findTest();
}

TEST_CASE("Txn/inmem/cursorInsertTest", "")
{
  InMemoryTxnFixture f;
  f.cursorInsertTest();
}

TEST_CASE("Txn/inmem/cursorEraseTest", "")
{
  InMemoryTxnFixture f;
  f.cursorEraseTest();
}

TEST_CASE("Txn/inmem/cursorFindTest", "")
{
  InMemoryTxnFixture f;
  f.cursorFindTest();
}

TEST_CASE("Txn/inmem/cursorGetDuplicateCountTest", "")
{
  InMemoryTxnFixture f;
  f.cursorGetDuplicateCountTest();
}

TEST_CASE("Txn/inmem/cursorGetRecordSizeTest", "")
{
  InMemoryTxnFixture f;
  f.cursorGetRecordSizeTest();
}

TEST_CASE("Txn/inmem/cursorOverwriteTest", "")
{
  InMemoryTxnFixture f;
  f.cursorOverwriteTest();
}

TEST_CASE("Txn/issue105Test", "")
{
  TxnFixture f;
  f.issue105Test();
}

} // namespace upscaledb
