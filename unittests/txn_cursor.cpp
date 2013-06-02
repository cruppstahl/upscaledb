/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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
#include "../src/cursor.h"
#include "../src/env.h"
#include "../src/os.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace hamsterdb;

class TxnCursorTest : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  TxnCursorTest()
  : hamsterDB_fixture("TxnCursorTest") {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(TxnCursorTest, cursorIsNilTest);
    BFC_REGISTER_TEST(TxnCursorTest, txnOpLinkedListTest);
    BFC_REGISTER_TEST(TxnCursorTest, getKeyFromCoupledCursorTest);
    BFC_REGISTER_TEST(TxnCursorTest, getKeyFromCoupledCursorUserAllocTest);
    BFC_REGISTER_TEST(TxnCursorTest, getKeyFromCoupledCursorEmptyKeyTest);
    BFC_REGISTER_TEST(TxnCursorTest, getKeyFromNilCursorTest);
    BFC_REGISTER_TEST(TxnCursorTest, getRecordFromCoupledCursorTest);
    BFC_REGISTER_TEST(TxnCursorTest, getRecordFromCoupledCursorUserAllocTest);
    BFC_REGISTER_TEST(TxnCursorTest, getRecordFromCoupledCursorEmptyRecordTest);
    BFC_REGISTER_TEST(TxnCursorTest, getRecordFromNilCursorTest);
    BFC_REGISTER_TEST(TxnCursorTest, findInsertTest);
    BFC_REGISTER_TEST(TxnCursorTest, findInsertEraseTest);
    BFC_REGISTER_TEST(TxnCursorTest, findInsertEraseOverwriteTest);
    BFC_REGISTER_TEST(TxnCursorTest, findCreateConflictTest);
    BFC_REGISTER_TEST(TxnCursorTest, moveFirstTest);
    BFC_REGISTER_TEST(TxnCursorTest, moveFirstInEmptyTreeTest);
    BFC_REGISTER_TEST(TxnCursorTest, moveNextWithNilCursorTest);
    BFC_REGISTER_TEST(TxnCursorTest, moveNextTest);
    BFC_REGISTER_TEST(TxnCursorTest, moveNextAfterEndTest);
    BFC_REGISTER_TEST(TxnCursorTest, moveNextSkipEraseTest);
    BFC_REGISTER_TEST(TxnCursorTest, moveNextSkipEraseInNodeTest);
    BFC_REGISTER_TEST(TxnCursorTest, moveLastTest);
    BFC_REGISTER_TEST(TxnCursorTest, moveLastInEmptyTreeTest);
    BFC_REGISTER_TEST(TxnCursorTest, movePrevWithNilCursorTest);
    BFC_REGISTER_TEST(TxnCursorTest, movePrevTest);
    BFC_REGISTER_TEST(TxnCursorTest, movePrevAfterEndTest);
    BFC_REGISTER_TEST(TxnCursorTest, movePrevSkipEraseTest);
    BFC_REGISTER_TEST(TxnCursorTest, movePrevSkipEraseInNodeTest);
    BFC_REGISTER_TEST(TxnCursorTest, insertKeysTest);
    BFC_REGISTER_TEST(TxnCursorTest, negativeInsertKeysTest);
    BFC_REGISTER_TEST(TxnCursorTest, insertOverwriteKeysTest);
    BFC_REGISTER_TEST(TxnCursorTest, insertCreateConflictTest);
    BFC_REGISTER_TEST(TxnCursorTest, eraseKeysTest);
    BFC_REGISTER_TEST(TxnCursorTest, negativeEraseKeysTest);
    BFC_REGISTER_TEST(TxnCursorTest, negativeEraseKeysNilTest);
    BFC_REGISTER_TEST(TxnCursorTest, overwriteRecordsTest);
    BFC_REGISTER_TEST(TxnCursorTest, overwriteRecordsNilCursorTest);
  }

protected:
  ham_cursor_t *m_cursor;
  ham_db_t *m_db;
  ham_env_t *m_env;

public:
  virtual void setup() {
    __super::setup();

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_RECOVERY | HAM_ENABLE_TRANSACTIONS, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 13, HAM_ENABLE_DUPLICATES, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_create(&m_cursor, m_db, 0, 0));
  }

  virtual void teardown() {
    __super::teardown();

    BFC_ASSERT_EQUAL(0, ham_cursor_close(m_cursor));
    BFC_ASSERT_EQUAL(0, ham_db_close(m_db, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, 0));
  }

  void cursorIsNilTest() {
    TransactionCursor cursor((Cursor *)0);

    BFC_ASSERT_EQUAL(true, cursor.is_nil());
    cursor.set_to_nil();
    BFC_ASSERT_EQUAL(true, cursor.is_nil());
  }

  void txnOpLinkedListTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t key = {0};
    ham_record_t record = {0};
    key.data = (void *)"hello";
    key.size = 5;

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = new TransactionNode((Database *)m_db, &key);
    op = node->append((Transaction *)txn,
            0, TransactionOperation::TXN_OP_INSERT_DUP, 55, &record);
    BFC_ASSERT(op != 0);

    TransactionCursor c1((Cursor *)0);
    c1.set_coupled_op(op);
    TransactionCursor c2(c1);
    TransactionCursor c3(c1);

    BFC_ASSERT_EQUAL((TransactionCursor *)0, op->get_cursors());

    op->add_cursor(&c1);
    BFC_ASSERT_EQUAL(&c1, op->get_cursors());
    BFC_ASSERT_EQUAL((TransactionCursor *)0, c1.get_coupled_previous());
    BFC_ASSERT_EQUAL((TransactionCursor *)0, c1.get_coupled_next());

    op->add_cursor(&c2);
    BFC_ASSERT_EQUAL(&c2, op->get_cursors());
    BFC_ASSERT_EQUAL(&c2, c1.get_coupled_previous());
    BFC_ASSERT_EQUAL(&c1, c2.get_coupled_next());
    BFC_ASSERT_EQUAL((TransactionCursor *)0, c2.get_coupled_previous());

    op->add_cursor(&c3);
    BFC_ASSERT_EQUAL(&c3, op->get_cursors());
    BFC_ASSERT_EQUAL(&c3, c2.get_coupled_previous());
    BFC_ASSERT_EQUAL(&c2, c3.get_coupled_next());
    BFC_ASSERT_EQUAL((TransactionCursor *)0, c3.get_coupled_previous());

    op->remove_cursor(&c2);
    BFC_ASSERT_EQUAL(&c3, op->get_cursors());
    BFC_ASSERT_EQUAL(&c3, c1.get_coupled_previous());
    BFC_ASSERT_EQUAL(&c1, c3.get_coupled_next());
    BFC_ASSERT_EQUAL((TransactionCursor *)0, c1.get_coupled_next());

    op->remove_cursor(&c3);
    BFC_ASSERT_EQUAL(&c1, op->get_cursors());
    BFC_ASSERT_EQUAL((TransactionCursor *)0, c1.get_coupled_previous());
    BFC_ASSERT_EQUAL((TransactionCursor *)0, c1.get_coupled_next());

    op->remove_cursor(&c1);
    BFC_ASSERT_EQUAL((TransactionCursor *)0, op->get_cursors());

    c1.set_to_nil();
    c2.set_to_nil();
    c3.set_to_nil();
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void getKeyFromCoupledCursorTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t k = {0};
    ham_key_t key = {0};
    ham_record_t record = {0};
    key.data = (void *)"hello";
    key.size = 5;

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = new TransactionNode((Database *)m_db, &key);
    op = node->append((Transaction *)txn,
            0, TransactionOperation::TXN_OP_INSERT_DUP, 55, &record);
    BFC_ASSERT(op != 0);

    TransactionCursor c((Cursor *)m_cursor);
    c.set_coupled_op(op);

    BFC_ASSERT_EQUAL(0, c.get_key(&k));
    BFC_ASSERT_EQUAL(k.size, key.size);
    BFC_ASSERT_EQUAL(0, memcmp(k.data, key.data, key.size));

    c.set_to_nil();
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void getKeyFromCoupledCursorUserAllocTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t k = {0};
    ham_key_t key = {0};
    ham_record_t record = {0};
    key.data = (void *)"hello";
    key.size = 5;

    char buffer[1024] = {0};
    k.data = &buffer[0];
    k.flags = HAM_KEY_USER_ALLOC;

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = new TransactionNode((Database *)m_db, &key);
    op = node->append((Transaction *)txn,
            0, TransactionOperation::TXN_OP_INSERT_DUP, 55, &record);
    BFC_ASSERT(op != 0);

    TransactionCursor c((Cursor *)m_cursor);
    c.set_coupled_op(op);

    BFC_ASSERT_EQUAL(0, c.get_key(&k));
    BFC_ASSERT_EQUAL(k.size, key.size);
    BFC_ASSERT_EQUAL(0, memcmp(k.data, key.data, key.size));

    ((Transaction *)txn)->free_ops();
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void getKeyFromCoupledCursorEmptyKeyTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t k = {0};
    ham_key_t key = {0};
    ham_record_t record = {0};

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = new TransactionNode((Database *)m_db, &key);
    op = node->append((Transaction *)txn,
            0, TransactionOperation::TXN_OP_INSERT_DUP, 55, &record);
    BFC_ASSERT(op!=0);

    TransactionCursor c((Cursor *)m_cursor);
    c.set_coupled_op(op);

    BFC_ASSERT_EQUAL(0, c.get_key(&k));
    BFC_ASSERT_EQUAL(k.size, key.size);
    BFC_ASSERT_EQUAL((void *)0, k.data);

    ((Transaction *)txn)->free_ops();
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void getKeyFromNilCursorTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t k = {0};
    ham_key_t key = {0};
    ham_record_t record = {0};
    key.data = (void *)"hello";
    key.size = 5;

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = new TransactionNode((Database *)m_db, &key);
    op = node->append((Transaction *)txn, 0,
            TransactionOperation::TXN_OP_INSERT_DUP, 55, &record);
    BFC_ASSERT(op != 0);

    TransactionCursor c((Cursor *)m_cursor);

    BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, c.get_key(&k));

    ((Transaction *)txn)->free_ops();
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void getRecordFromCoupledCursorTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t key = {0};
    ham_record_t r = {0};
    ham_record_t record = {0};
    record.data = (void *)"hello";
    record.size = 5;

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = new TransactionNode((Database *)m_db, &key);
    op = node->append((Transaction *)txn, 0,
            TransactionOperation::TXN_OP_INSERT_DUP, 55, &record);
    BFC_ASSERT(op!=0);

    TransactionCursor c((Cursor *)m_cursor);
    c.set_coupled_op(op);

    BFC_ASSERT_EQUAL(0, c.get_record(&r));
    BFC_ASSERT_EQUAL(r.size, record.size);
    BFC_ASSERT_EQUAL(0, memcmp(r.data, record.data, record.size));

    ((Transaction *)txn)->free_ops();
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void getRecordFromCoupledCursorUserAllocTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t key = {0};
    ham_record_t r = {0};
    ham_record_t record = {0};
    record.data = (void *)"hello";
    record.size = 5;

    char buffer[1024] = {0};
    r.data = &buffer[0];
    r.flags = HAM_RECORD_USER_ALLOC;

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = new TransactionNode((Database *)m_db, &key);
    op = node->append((Transaction *)txn, 0,
            TransactionOperation::TXN_OP_INSERT_DUP, 55, &record);
    BFC_ASSERT(op!=0);

    TransactionCursor c((Cursor *)m_cursor);
    c.set_coupled_op(op);

    BFC_ASSERT_EQUAL(0, c.get_record(&r));
    BFC_ASSERT_EQUAL(r.size, record.size);
    BFC_ASSERT_EQUAL(0, memcmp(r.data, record.data, record.size));

    ((Transaction *)txn)->free_ops();
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void getRecordFromCoupledCursorEmptyRecordTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t key = {0};
    ham_record_t record = {0};
    ham_record_t r = {0};

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = new TransactionNode((Database *)m_db, &key);
    op = node->append((Transaction *)txn, 0,
            TransactionOperation::TXN_OP_INSERT_DUP, 55, &record);
    BFC_ASSERT(op!=0);

    TransactionCursor c((Cursor *)m_cursor);
    c.set_coupled_op(op);

    BFC_ASSERT_EQUAL(0, c.get_record(&r));
    BFC_ASSERT_EQUAL(r.size, record.size);
    BFC_ASSERT_EQUAL((void *)0, r.data);

    ((Transaction *)txn)->free_ops();
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void getRecordFromNilCursorTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t key = {0};
    ham_record_t record = {0};
    ham_record_t r = {0};

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = new TransactionNode((Database *)m_db, &key);
    op = node->append((Transaction *)txn, 0,
            TransactionOperation::TXN_OP_INSERT_DUP, 55, &record);
    BFC_ASSERT(op!=0);

    TransactionCursor c((Cursor *)m_cursor);

    BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, c.get_record(&r));

    ((Transaction *)txn)->free_ops();
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  ham_status_t insert(ham_txn_t *txn, const char *key, const char *record = 0,
          ham_u32_t flags = 0) {
    ham_key_t k = {0};
    if (key) {
      k.data = (void *)key;
      k.size = strlen(key) + 1;
    }
    ham_record_t r = {0};
    if (record) {
      r.data = (void *)record;
      r.size = sizeof(record);
    }
    return (ham_db_insert(m_db, txn, &k, &r, flags));
  }

  ham_status_t insertCursor(TransactionCursor *cursor, const char *key,
          const char *record = 0, ham_u32_t flags = 0) {
    ham_key_t k = {0};
    if (key) {
      k.data = (void *)key;
      k.size = strlen(key) + 1;
    }
    ham_record_t r = {0};
    if (record) {
      r.data = (void *)record;
      r.size = strlen(record) + 1;
    }
    return (cursor->insert(&k, &r, flags));
  }

  ham_status_t overwriteCursor(TransactionCursor *cursor, const char *record) {
    ham_record_t r = {0};
    if (record) {
      r.data = (void *)record;
      r.size = strlen(record) + 1;
    }
    return (cursor->overwrite(&r));
  }

  ham_status_t erase(ham_txn_t *txn, const char *key) {
    ham_key_t k = {0};
    if (key) {
      k.data = (void *)key;
      k.size = strlen(key) + 1;
    }
    return (ham_db_erase(m_db, txn, &k, 0));
  }

  ham_status_t findCursor(TransactionCursor *cursor, const char *key,
          const char *record = 0) {
    ham_key_t k = {0};
    if (key) {
      k.data = (void *)key;
      k.size = strlen(key) + 1;
    }
    ham_status_t st = cursor->find(&k, 0);
    if (st)
      return (st);
    if (record) {
      ham_record_t r = {0};
      BFC_ASSERT_EQUAL(0, cursor->get_record(&r));
      BFC_ASSERT_EQUAL(r.size, strlen(record) + 1);
      BFC_ASSERT_EQUAL(0, memcmp(r.data, record, r.size));
    }
    return (0);
  }

  ham_status_t moveCursor(TransactionCursor *cursor, const char *key,
          ham_u32_t flags) {
    ham_key_t k = {0};
    ham_status_t st = cursor->move(flags);
    if (st)
      return (st);
    st = cursor->get_key(&k);
    if (st)
      return (st);
    if (key) {
      if (strcmp((char *)k.data, key))
        return (HAM_INTERNAL_ERROR);
    }
    else {
      if (k.size != 0)
        return (HAM_INTERNAL_ERROR);
    }
    return (HAM_SUCCESS);
  }

  void findInsertEraseTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert two different keys, delete the first one */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
    BFC_ASSERT_EQUAL(0, erase(txn, "key1"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));

    /* find the first key - fails */
    BFC_ASSERT_EQUAL(HAM_KEY_ERASED_IN_TXN, findCursor(cursor, "key1"));

    /* insert it again */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1"));

    /* find second key */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key2"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void findInsertEraseOverwriteTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a key and overwrite it twice */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1", "rec1"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key1", "rec2", HAM_OVERWRITE));
    BFC_ASSERT_EQUAL(0, insert(txn, "key1", "rec3", HAM_OVERWRITE));

    /* find the first key */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1"));

    /* erase it, then insert it again */
    BFC_ASSERT_EQUAL(0, erase(txn, "key1"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key1", "rec4", HAM_OVERWRITE));
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void findInsertTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert two different keys */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));

    /* find the first key */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1"));

    /* now the cursor is coupled to this key */
    BFC_ASSERT(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ham_key_t *key = op->get_node()->get_key();
    BFC_ASSERT_EQUAL(5, key->size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key1"));

    /* now the key is coupled; find second key */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key2"));

    /* and the cursor is still coupled */
    BFC_ASSERT(!cursor->is_nil());
    op = cursor->get_coupled_op();
    key = op->get_node()->get_key();
    BFC_ASSERT_EQUAL(5, key->size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key2"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void moveFirstTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a few different keys */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

    /* find the first key (with a nil cursor) */
    BFC_ASSERT_EQUAL(0, moveCursor(cursor, "key1", HAM_CURSOR_FIRST));

    /* now the cursor is coupled to this key */
    BFC_ASSERT(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ham_key_t *key = op->get_node()->get_key();
    BFC_ASSERT_EQUAL(5, key->size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key1"));

    /* do it again with a coupled cursor */
    BFC_ASSERT_EQUAL(0, moveCursor(cursor, "key1", HAM_CURSOR_FIRST));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void moveFirstInEmptyTreeTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* find the first key */
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          moveCursor(cursor, "key1", HAM_CURSOR_FIRST));

    /* now the cursor is nil */
    BFC_ASSERT_EQUAL(true, cursor->is_nil());

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void findCreateConflictTest() {
    ham_txn_t *txn, *txn2;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a key, then erase it */
    BFC_ASSERT_EQUAL(0, insert(txn2, "key1"));
    BFC_ASSERT_EQUAL(HAM_TXN_CONFLICT, findCursor(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

  void moveNextWithNilCursorTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* make sure that the cursor is nil */
    BFC_ASSERT_EQUAL(true, cursor->is_nil());

    BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL,
          moveCursor(cursor, 0, HAM_CURSOR_NEXT));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void moveNextTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a few different keys */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

    /* find the first key */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1"));

    /* move next */
    BFC_ASSERT_EQUAL(0, moveCursor(cursor, "key2", HAM_CURSOR_NEXT));

    /* now the cursor is coupled to this key */
    BFC_ASSERT(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ham_key_t *key = op->get_node()->get_key();
    BFC_ASSERT_EQUAL(5, key->size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key2"));

    /* now the key is coupled; move next once more */
    BFC_ASSERT_EQUAL(0, moveCursor(cursor, "key3", HAM_CURSOR_NEXT));

    /* and the cursor is still coupled */
    BFC_ASSERT(!cursor->is_nil());
    op = cursor->get_coupled_op();
    key = op->get_node()->get_key();
    BFC_ASSERT_EQUAL(5, key->size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key3"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void moveNextAfterEndTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert one key */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));

    /* find the first key */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1"));

    /* move next */
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          moveCursor(cursor, "key2", HAM_CURSOR_NEXT));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void moveNextSkipEraseTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/erase keys */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
    BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

    /* find the first key */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1"));

    /* move next */
    BFC_ASSERT_EQUAL(HAM_KEY_ERASED_IN_TXN,
          moveCursor(cursor, 0, HAM_CURSOR_NEXT));

    /* move next */
    BFC_ASSERT_EQUAL(0, moveCursor(cursor, "key3", HAM_CURSOR_NEXT));

    /* reached the end */
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          moveCursor(cursor, "key3", HAM_CURSOR_NEXT));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void moveNextSkipEraseInNodeTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/erase keys */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
    BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
    BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
    BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

    /* find the first key */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1"));

    /* move next */
    BFC_ASSERT_EQUAL(HAM_KEY_ERASED_IN_TXN,
          moveCursor(cursor, 0, HAM_CURSOR_NEXT));

    /* move next */
    BFC_ASSERT_EQUAL(0, moveCursor(cursor, "key3", HAM_CURSOR_NEXT));

    /* reached the end */
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          moveCursor(cursor, "key3", HAM_CURSOR_NEXT));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void moveLastTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a few different keys */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

    /* find the last key (with a nil cursor) */
    BFC_ASSERT_EQUAL(0, moveCursor(cursor, "key3", HAM_CURSOR_LAST));

    /* now the cursor is coupled to this key */
    BFC_ASSERT(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ham_key_t *key = op->get_node()->get_key();
    BFC_ASSERT_EQUAL(5, key->size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key3"));

    /* do it again with a coupled cursor */
    BFC_ASSERT_EQUAL(0, moveCursor(cursor, "key3", HAM_CURSOR_LAST));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void moveLastInEmptyTreeTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* find the first key */
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          moveCursor(cursor, "key1", HAM_CURSOR_LAST));

    /* now the cursor is nil */
    BFC_ASSERT_EQUAL(true, cursor->is_nil());

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void movePrevWithNilCursorTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* make sure that the cursor is nil */
    BFC_ASSERT_EQUAL(true, cursor->is_nil());

    BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL,
          moveCursor(cursor, 0, HAM_CURSOR_PREVIOUS));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void movePrevTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a few different keys */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

    /* find the last key */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key3"));

    /* move previous */
    BFC_ASSERT_EQUAL(0, moveCursor(cursor, "key2", HAM_CURSOR_PREVIOUS));

    /* now the cursor is coupled to this key */
    BFC_ASSERT(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ham_key_t *key = op->get_node()->get_key();
    BFC_ASSERT_EQUAL(5, key->size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key2"));

    /* now the key is coupled; move previous once more */
    BFC_ASSERT_EQUAL(0, moveCursor(cursor, "key1", HAM_CURSOR_PREVIOUS));

    /* and the cursor is still coupled */
    BFC_ASSERT(!cursor->is_nil());
    op = cursor->get_coupled_op();
    key = op->get_node()->get_key();
    BFC_ASSERT_EQUAL(5, key->size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void movePrevAfterEndTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert one key */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));

    /* find the first key */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1"));

    /* move previous */
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          moveCursor(cursor, "key2", HAM_CURSOR_PREVIOUS));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void movePrevSkipEraseTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/erase keys */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
    BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

    /* find the first key */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key3"));

    /* move previous */
    BFC_ASSERT_EQUAL(HAM_KEY_ERASED_IN_TXN,
          moveCursor(cursor, 0, HAM_CURSOR_PREVIOUS));

    /* move previous */
    BFC_ASSERT_EQUAL(0, moveCursor(cursor, "key1", HAM_CURSOR_PREVIOUS));

    /* reached the end */
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          moveCursor(cursor, "key1", HAM_CURSOR_PREVIOUS));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void movePrevSkipEraseInNodeTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/erase keys */
    BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
    BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
    BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
    BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
    BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

    /* find the last key */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key3"));

    /* move previous */
    BFC_ASSERT_EQUAL(HAM_KEY_ERASED_IN_TXN,
          moveCursor(cursor, 0, HAM_CURSOR_PREVIOUS));

    /* move previous */
    BFC_ASSERT_EQUAL(0, moveCursor(cursor, "key1", HAM_CURSOR_PREVIOUS));

    /* reached the end */
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          moveCursor(cursor, "key1", HAM_CURSOR_PREVIOUS));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  bool cursorIsCoupled(TransactionCursor *cursor, const char *k) {
    BFC_ASSERT(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ham_key_t *key = op->get_node()->get_key();
    if (strlen(k) + 1 != key->size)
      return (false);
    if (strcmp((char *)key->data, k))
      return (false);
    return (true);
  }

  void insertKeysTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a few different keys */
    BFC_ASSERT_EQUAL(0, insertCursor(cursor, "key1"));
    BFC_ASSERT_EQUAL(0, insertCursor(cursor, "key2"));
    BFC_ASSERT_EQUAL(0, insertCursor(cursor, "key3"));

    /* make sure that the keys exist and that the cursor is coupled */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1"));
    BFC_ASSERT_EQUAL(true, cursorIsCoupled(cursor, "key1"));
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key2"));
    BFC_ASSERT_EQUAL(true, cursorIsCoupled(cursor, "key2"));
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key3"));
    BFC_ASSERT_EQUAL(true, cursorIsCoupled(cursor, "key3"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void negativeInsertKeysTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a key twice - creates a duplicate key */
    BFC_ASSERT_EQUAL(0, insertCursor(cursor, "key1"));
    BFC_ASSERT_EQUAL(HAM_DUPLICATE_KEY, insertCursor(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void insertOverwriteKeysTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/overwrite keys */
    BFC_ASSERT_EQUAL(0, insertCursor(cursor, "key1"));
    BFC_ASSERT_EQUAL(0, insertCursor(cursor, "key1", 0, HAM_OVERWRITE));
    BFC_ASSERT_EQUAL(0, insertCursor(cursor, "key1", 0, HAM_OVERWRITE));

    /* make sure that the key exists and that the cursor is coupled */
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1"));
    BFC_ASSERT_EQUAL(true, cursorIsCoupled(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void insertCreateConflictTest() {
    ham_txn_t *txn, *txn2;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/overwrite keys */
    BFC_ASSERT_EQUAL(0, insert(txn2, "key1"));
    BFC_ASSERT_EQUAL(HAM_TXN_CONFLICT, insertCursor(cursor, "key1"));

    /* cursor must be nil */
    BFC_ASSERT_EQUAL(true, cursor->is_nil());

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
  }

  void eraseKeysTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/erase a few different keys */
    BFC_ASSERT_EQUAL(0, insertCursor(cursor, "key1"));
    BFC_ASSERT_EQUAL(true, cursorIsCoupled(cursor, "key1"));
    BFC_ASSERT_EQUAL(false, cursor->is_nil());
    BFC_ASSERT_EQUAL(0, cursor->erase());

    /* make sure that the keys do not exist */
    BFC_ASSERT_EQUAL(HAM_KEY_ERASED_IN_TXN, findCursor(cursor, "key1"));

    BFC_ASSERT_EQUAL(0, insertCursor(cursor, "key2"));
    BFC_ASSERT_EQUAL(0, cursor->erase());
    BFC_ASSERT_EQUAL(HAM_KEY_ERASED_IN_TXN, findCursor(cursor, "key2"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void negativeEraseKeysTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* erase a key that does not exist */
    BFC_ASSERT_EQUAL(0, insertCursor(cursor, "key1"));
    BFC_ASSERT_EQUAL(0, erase(txn, "key1"));
    BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, cursor->erase());

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void negativeEraseKeysNilTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* erase a key with a cursor that is nil */
    BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, cursor->erase());

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void overwriteRecordsTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a key and overwrite the record */
    BFC_ASSERT_EQUAL(0, insertCursor(cursor, "key1", "rec1"));
    BFC_ASSERT_EQUAL(true, cursorIsCoupled(cursor, "key1"));
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1", "rec1"));
    BFC_ASSERT_EQUAL(true, cursorIsCoupled(cursor, "key1"));
    BFC_ASSERT_EQUAL(0, overwriteCursor(cursor, "rec2"));
    BFC_ASSERT_EQUAL(true, cursorIsCoupled(cursor, "key1"));
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1", "rec2"));
    BFC_ASSERT_EQUAL(true, cursorIsCoupled(cursor, "key1"));
    BFC_ASSERT_EQUAL(0, overwriteCursor(cursor, "rec3"));
    BFC_ASSERT_EQUAL(true, cursorIsCoupled(cursor, "key1"));
    BFC_ASSERT_EQUAL(0, findCursor(cursor, "key1", "rec3"));
    BFC_ASSERT_EQUAL(true, cursorIsCoupled(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void overwriteRecordsNilCursorTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, overwriteCursor(cursor, "rec2"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }
};

BFC_REGISTER_FIXTURE(TxnCursorTest);

