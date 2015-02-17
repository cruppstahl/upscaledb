/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "1base/error.h"
#include "1os/os.h"
#include "2page/page.h"
#include "4context/context.h"
#include "4cursor/cursor.h"
#include "4db/db.h"
#include "4env/env.h"
#include "4txn/txn.h"
#include "4txn/txn_cursor.h"

namespace hamsterdb {

struct TxnCursorFixture {
  ham_cursor_t *m_cursor;
  ham_db_t *m_db;
  ham_env_t *m_env;
  ScopedPtr<Context> m_context;

  TxnCursorFixture()
    : m_cursor(0), m_db(0), m_env(0) {
    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
            HAM_ENABLE_RECOVERY | HAM_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 13, HAM_ENABLE_DUPLICATE_KEYS, 0));
    REQUIRE(0 == ham_cursor_create(&m_cursor, m_db, 0, 0));
    m_context.reset(new Context((LocalEnvironment *)m_env, 0, 0));
  }

  ~TxnCursorFixture() {
    m_context->changeset.clear();
    REQUIRE(0 == ham_cursor_close(m_cursor));
    REQUIRE(0 == ham_db_close(m_db, 0));
    REQUIRE(0 == ham_env_close(m_env, 0));
  }

  TransactionNode *create_transaction_node(ham_key_t *key) {
    LocalDatabase *ldb = (LocalDatabase *)m_db;
    TransactionNode *node = new TransactionNode(ldb, key);
    ldb->txn_index()->store(node);
    return (node);
  }

  void cursorIsNilTest() {
    TransactionCursor cursor((Cursor *)0);

    REQUIRE(true == cursor.is_nil());
    cursor.set_to_nil();
    REQUIRE(true == cursor.is_nil());
  }

  void getKeyFromCoupledCursorTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t k = {0};
    ham_record_t record = {0};
    ham_key_t key = ham_make_key((void *)"hello", 5);

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn,
                0, TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != 0);

    TransactionCursor c((Cursor *)m_cursor);
    c.m_coupled_op = op;

    c.copy_coupled_key(&k);
    REQUIRE(k.size == key.size);
    REQUIRE(0 == memcmp(k.data, key.data, key.size));

    c.set_to_nil();
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void getKeyFromCoupledCursorUserAllocTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_record_t record = {0};
    ham_key_t key = ham_make_key((void *)"hello", 5);

    char buffer[1024] = {0};
    ham_key_t k = ham_make_key(&buffer[0], 0);
    k.flags = HAM_KEY_USER_ALLOC;

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn,
                0, TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != 0);

    TransactionCursor c((Cursor *)m_cursor);
    c.m_coupled_op = op;

    c.copy_coupled_key(&k);
    REQUIRE(k.size == key.size);
    REQUIRE(0 == memcmp(k.data, key.data, key.size));

    c.set_to_nil();
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void getKeyFromCoupledCursorEmptyKeyTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t k = {0};
    ham_key_t key = {0};
    ham_record_t record = {0};

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn,
                0, TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op!=0);

    TransactionCursor c((Cursor *)m_cursor);
    c.m_coupled_op = op;

    c.copy_coupled_key(&k);
    REQUIRE(k.size == key.size);
    REQUIRE((void *)0 == k.data);

    c.set_to_nil();
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void getKeyFromNilCursorTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t k = {0};
    ham_record_t record = {0};
    ham_key_t key = ham_make_key((void *)"hello", 5);

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn, 0,
                TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != 0);

    TransactionCursor c((Cursor *)m_cursor);

    REQUIRE_CATCH(c.copy_coupled_key(&k), HAM_CURSOR_IS_NIL);

    c.set_to_nil();
    REQUIRE(0 == ham_txn_commit(txn, 0));
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

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn, 0,
                TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op!=0);

    TransactionCursor c((Cursor *)m_cursor);
    c.m_coupled_op = op;

    c.copy_coupled_record(&r);
    REQUIRE(r.size == record.size);
    REQUIRE(0 == memcmp(r.data, record.data, record.size));

    c.set_to_nil();
    REQUIRE(0 == ham_txn_commit(txn, 0));
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

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn, 0,
                TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op!=0);

    TransactionCursor c((Cursor *)m_cursor);
    c.m_coupled_op = op;

    c.copy_coupled_record(&r);
    REQUIRE(r.size == record.size);
    REQUIRE(0 == memcmp(r.data, record.data, record.size));

    c.set_to_nil();
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void getRecordFromCoupledCursorEmptyRecordTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t key = {0};
    ham_record_t record = {0};
    ham_record_t r = {0};

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn, 0,
                TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op!=0);

    TransactionCursor c((Cursor *)m_cursor);
    c.m_coupled_op = op;

    c.copy_coupled_record(&r);
    REQUIRE(r.size == record.size);
    REQUIRE((void *)0 == r.data);

    c.set_to_nil();
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void getRecordFromNilCursorTest() {
    ham_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ham_key_t key = {0};
    ham_record_t record = {0};
    ham_record_t r = {0};

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn, 0,
                TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op!=0);

    TransactionCursor c((Cursor *)m_cursor);

    REQUIRE_CATCH(c.copy_coupled_record(&r), HAM_CURSOR_IS_NIL);

    c.set_to_nil();
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  ham_status_t insert(ham_txn_t *txn, const char *key, const char *record = 0,
          uint32_t flags = 0) {
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
          const char *record = 0, uint32_t flags = 0) {
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
    return (cursor->test_insert(&k, &r, flags));
  }

  ham_status_t overwriteCursor(TransactionCursor *cursor, const char *record) {
    ham_record_t r = {0};
    if (record) {
      r.data = (void *)record;
      r.size = strlen(record) + 1;
    }

    m_context->txn = (LocalTransaction *)cursor->get_parent()->get_txn();
    return (cursor->overwrite(m_context.get(), m_context->txn, &r));
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
      cursor->copy_coupled_record(&r);
      REQUIRE(r.size == strlen(record) + 1);
      REQUIRE(0 == memcmp(r.data, record, r.size));
    }
    return (0);
  }

  ham_status_t moveCursor(TransactionCursor *cursor, const char *key,
          uint32_t flags) {
    ham_key_t k = {0};
    ham_status_t st = cursor->move(flags);
    if (st)
      return (st);
    cursor->copy_coupled_key(&k);
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

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert two different keys, delete the first one */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == erase(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));

    /* find the first key - fails */
    REQUIRE(HAM_KEY_ERASED_IN_TXN == findCursor(cursor, "key1"));

    /* insert it again */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* find second key */
    REQUIRE(0 == findCursor(cursor, "key2"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void findInsertEraseOverwriteTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a key and overwrite it twice */
    REQUIRE(0 == insert(txn, "key1", "rec1"));
    REQUIRE(0 == insert(txn, "key1", "rec2", HAM_OVERWRITE));
    REQUIRE(0 == insert(txn, "key1", "rec3", HAM_OVERWRITE));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* erase it, then insert it again */
    REQUIRE(0 == erase(txn, "key1"));
    REQUIRE(0 == insert(txn, "key1", "rec4", HAM_OVERWRITE));
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void findInsertTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert two different keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* now the cursor is coupled to this key */
    REQUIRE(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ham_key_t *key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key1"));

    /* now the key is coupled; find second key */
    REQUIRE(0 == findCursor(cursor, "key2"));

    /* and the cursor is still coupled */
    REQUIRE(!cursor->is_nil());
    op = cursor->get_coupled_op();
    key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key2"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void moveFirstTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a few different keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the first key (with a nil cursor) */
    REQUIRE(0 == moveCursor(cursor, "key1", HAM_CURSOR_FIRST));

    /* now the cursor is coupled to this key */
    REQUIRE(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ham_key_t *key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key1"));

    /* do it again with a coupled cursor */
    REQUIRE(0 == moveCursor(cursor, "key1", HAM_CURSOR_FIRST));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void moveFirstInEmptyTreeTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* find the first key */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          moveCursor(cursor, "key1", HAM_CURSOR_FIRST));

    /* now the cursor is nil */
    REQUIRE(true == cursor->is_nil());

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void findCreateConflictTest() {
    ham_txn_t *txn, *txn2;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_txn_begin(&txn2, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a key, then erase it */
    REQUIRE(0 == insert(txn2, "key1"));
    REQUIRE(HAM_TXN_CONFLICT == findCursor(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
    REQUIRE(0 == ham_txn_commit(txn2, 0));
  }

  void moveNextWithNilCursorTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* make sure that the cursor is nil */
    REQUIRE(true == cursor->is_nil());

    REQUIRE(HAM_CURSOR_IS_NIL ==
          moveCursor(cursor, 0, HAM_CURSOR_NEXT));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void moveNextTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a few different keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* move next */
    REQUIRE(0 == moveCursor(cursor, "key2", HAM_CURSOR_NEXT));

    /* now the cursor is coupled to this key */
    REQUIRE(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ham_key_t *key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key2"));

    /* now the key is coupled; move next once more */
    REQUIRE(0 == moveCursor(cursor, "key3", HAM_CURSOR_NEXT));

    /* and the cursor is still coupled */
    REQUIRE(!cursor->is_nil());
    op = cursor->get_coupled_op();
    key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key3"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void moveNextAfterEndTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert one key */
    REQUIRE(0 == insert(txn, "key1"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* move next */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          moveCursor(cursor, "key2", HAM_CURSOR_NEXT));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void moveNextSkipEraseTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/erase keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == erase(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* move next */
    REQUIRE(HAM_KEY_ERASED_IN_TXN ==
          moveCursor(cursor, 0, HAM_CURSOR_NEXT));

    /* move next */
    REQUIRE(0 == moveCursor(cursor, "key3", HAM_CURSOR_NEXT));

    /* reached the end */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          moveCursor(cursor, "key3", HAM_CURSOR_NEXT));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void moveNextSkipEraseInNodeTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/erase keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == erase(txn, "key2"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == erase(txn, "key2"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == erase(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* move next */
    REQUIRE(HAM_KEY_ERASED_IN_TXN ==
          moveCursor(cursor, 0, HAM_CURSOR_NEXT));

    /* move next */
    REQUIRE(0 == moveCursor(cursor, "key3", HAM_CURSOR_NEXT));

    /* reached the end */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          moveCursor(cursor, "key3", HAM_CURSOR_NEXT));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void moveLastTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a few different keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the last key (with a nil cursor) */
    REQUIRE(0 == moveCursor(cursor, "key3", HAM_CURSOR_LAST));

    /* now the cursor is coupled to this key */
    REQUIRE(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ham_key_t *key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key3"));

    /* do it again with a coupled cursor */
    REQUIRE(0 == moveCursor(cursor, "key3", HAM_CURSOR_LAST));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void moveLastInEmptyTreeTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* find the first key */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          moveCursor(cursor, "key1", HAM_CURSOR_LAST));

    /* now the cursor is nil */
    REQUIRE(true == cursor->is_nil());

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void movePrevWithNilCursorTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* make sure that the cursor is nil */
    REQUIRE(true == cursor->is_nil());

    REQUIRE(HAM_CURSOR_IS_NIL ==
          moveCursor(cursor, 0, HAM_CURSOR_PREVIOUS));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void movePrevTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a few different keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the last key */
    REQUIRE(0 == findCursor(cursor, "key3"));

    /* move previous */
    REQUIRE(0 == moveCursor(cursor, "key2", HAM_CURSOR_PREVIOUS));

    /* now the cursor is coupled to this key */
    REQUIRE(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ham_key_t *key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key2"));

    /* now the key is coupled; move previous once more */
    REQUIRE(0 == moveCursor(cursor, "key1", HAM_CURSOR_PREVIOUS));

    /* and the cursor is still coupled */
    REQUIRE(!cursor->is_nil());
    op = cursor->get_coupled_op();
    key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void movePrevAfterEndTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert one key */
    REQUIRE(0 == insert(txn, "key1"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* move previous */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          moveCursor(cursor, "key2", HAM_CURSOR_PREVIOUS));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void movePrevSkipEraseTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/erase keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == erase(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key3"));

    /* move previous */
    REQUIRE(HAM_KEY_ERASED_IN_TXN ==
          moveCursor(cursor, 0, HAM_CURSOR_PREVIOUS));

    /* move previous */
    REQUIRE(0 == moveCursor(cursor, "key1", HAM_CURSOR_PREVIOUS));

    /* reached the end */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          moveCursor(cursor, "key1", HAM_CURSOR_PREVIOUS));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void movePrevSkipEraseInNodeTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/erase keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == erase(txn, "key2"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == erase(txn, "key2"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == erase(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the last key */
    REQUIRE(0 == findCursor(cursor, "key3"));

    /* move previous */
    REQUIRE(HAM_KEY_ERASED_IN_TXN ==
          moveCursor(cursor, 0, HAM_CURSOR_PREVIOUS));

    /* move previous */
    REQUIRE(0 == moveCursor(cursor, "key1", HAM_CURSOR_PREVIOUS));

    /* reached the end */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          moveCursor(cursor, "key1", HAM_CURSOR_PREVIOUS));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  bool cursorIsCoupled(TransactionCursor *cursor, const char *k) {
    REQUIRE(!cursor->is_nil());
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

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a few different keys */
    REQUIRE(0 == insertCursor(cursor, "key1"));
    REQUIRE(0 == insertCursor(cursor, "key2"));
    REQUIRE(0 == insertCursor(cursor, "key3"));

    /* make sure that the keys exist and that the cursor is coupled */
    REQUIRE(0 == findCursor(cursor, "key1"));
    REQUIRE(true == cursorIsCoupled(cursor, "key1"));
    REQUIRE(0 == findCursor(cursor, "key2"));
    REQUIRE(true == cursorIsCoupled(cursor, "key2"));
    REQUIRE(0 == findCursor(cursor, "key3"));
    REQUIRE(true == cursorIsCoupled(cursor, "key3"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void negativeInsertKeysTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a key twice - creates a duplicate key */
    REQUIRE(0 == insertCursor(cursor, "key1"));
    REQUIRE(HAM_DUPLICATE_KEY == insertCursor(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void insertOverwriteKeysTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/overwrite keys */
    REQUIRE(0 == insertCursor(cursor, "key1"));
    REQUIRE(0 == insertCursor(cursor, "key1", 0, HAM_OVERWRITE));
    REQUIRE(0 == insertCursor(cursor, "key1", 0, HAM_OVERWRITE));

    /* make sure that the key exists and that the cursor is coupled */
    REQUIRE(0 == findCursor(cursor, "key1"));
    REQUIRE(true == cursorIsCoupled(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void insertCreateConflictTest() {
    ham_txn_t *txn, *txn2;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_txn_begin(&txn2, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert/overwrite keys */
    REQUIRE(0 == insert(txn2, "key1"));
    REQUIRE(HAM_TXN_CONFLICT == insertCursor(cursor, "key1"));

    /* cursor must be nil */
    REQUIRE(true == cursor->is_nil());

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
    REQUIRE(0 == ham_txn_commit(txn2, 0));
  }

  void overwriteRecordsTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    /* insert a key and overwrite the record */
    REQUIRE(0 == insertCursor(cursor, "key1", "rec1"));
    REQUIRE(true == cursorIsCoupled(cursor, "key1"));
    REQUIRE(0 == findCursor(cursor, "key1", "rec1"));
    REQUIRE(true == cursorIsCoupled(cursor, "key1"));
    REQUIRE(0 == overwriteCursor(cursor, "rec2"));
    REQUIRE(true == cursorIsCoupled(cursor, "key1"));
    REQUIRE(0 == findCursor(cursor, "key1", "rec2"));
    REQUIRE(true == cursorIsCoupled(cursor, "key1"));
    REQUIRE(0 == overwriteCursor(cursor, "rec3"));
    REQUIRE(true == cursorIsCoupled(cursor, "key1"));
    REQUIRE(0 == findCursor(cursor, "key1", "rec3"));
    REQUIRE(true == cursorIsCoupled(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void overwriteRecordsNilCursorTest() {
    ham_txn_t *txn;

    TransactionCursor *cursor = ((Cursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->set_txn((Transaction *)txn);

    REQUIRE(HAM_CURSOR_IS_NIL == overwriteCursor(cursor, "rec2"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->set_txn(0);

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void approxMatchTest() {
    ham_db_t *db;
    ham_parameter_t params[] = {
      {HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT64},
      {0, 0}
    };
    REQUIRE(0 == ham_env_create_db(m_env, &db, 33, 0, &params[0]));

    char data[1024 * 64] = {0};
    for (int i = 0; i < 40; i++) {
      uint64_t k = 10 + i * 13;
      ham_key_t key = ham_make_key(&k, sizeof(k));
      ham_record_t record = ham_make_record(data, sizeof(data));
      REQUIRE(ham_db_insert(db, 0, &key, &record, 0) == 0);
    }

    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, db, 0, 0));

    {
      uint64_t k = 0;
      ham_key_t key = ham_make_key(&k, sizeof(k));
      ham_record_t record = {0};
      REQUIRE(0 == ham_db_find(db, 0, &key, &record, HAM_FIND_GEQ_MATCH));
      REQUIRE(key.size == 8);
      REQUIRE(*(uint64_t *)key.data == 10);
    }

    {
      uint64_t k = 0;
      ham_key_t key = ham_make_key(&k, sizeof(k));
      ham_record_t record = {0};
      REQUIRE(0 == ham_cursor_find(cursor, &key, &record, HAM_FIND_GEQ_MATCH));
      REQUIRE(key.size == 8);
      REQUIRE(*(uint64_t *)key.data == 10);
    }

    REQUIRE(0 == ham_db_close(db, HAM_AUTO_CLEANUP));
  }
};

TEST_CASE("TxnCursor/cursorIsNilTest", "")
{
  TxnCursorFixture f;
  f.cursorIsNilTest();
}

TEST_CASE("TxnCursor/getKeyFromCoupledCursorTest", "")
{
  TxnCursorFixture f;
  f.getKeyFromCoupledCursorTest();
}

TEST_CASE("TxnCursor/getKeyFromCoupledCursorUserAllocTest", "")
{
  TxnCursorFixture f;
  f.getKeyFromCoupledCursorUserAllocTest();
}

TEST_CASE("TxnCursor/getKeyFromCoupledCursorEmptyKeyTest", "")
{
  TxnCursorFixture f;
  f.getKeyFromCoupledCursorEmptyKeyTest();
}

TEST_CASE("TxnCursor/getKeyFromNilCursorTest", "")
{
  TxnCursorFixture f;
  f.getKeyFromNilCursorTest();
}

TEST_CASE("TxnCursor/getRecordFromCoupledCursorTest", "")
{
  TxnCursorFixture f;
  f.getRecordFromCoupledCursorTest();
}

TEST_CASE("TxnCursor/getRecordFromCoupledCursorUserAllocTest", "")
{
  TxnCursorFixture f;
  f.getRecordFromCoupledCursorUserAllocTest();
}

TEST_CASE("TxnCursor/getRecordFromCoupledCursorEmptyRecordTest", "")
{
  TxnCursorFixture f;
  f.getRecordFromCoupledCursorEmptyRecordTest();
}

TEST_CASE("TxnCursor/getRecordFromNilCursorTest", "")
{
  TxnCursorFixture f;
  f.getRecordFromNilCursorTest();
}

TEST_CASE("TxnCursor/findInsertTest", "")
{
  TxnCursorFixture f;
  f.findInsertTest();
}

TEST_CASE("TxnCursor/findInsertEraseTest", "")
{
  TxnCursorFixture f;
  f.findInsertEraseTest();
}

TEST_CASE("TxnCursor/findInsertEraseOverwriteTest", "")
{
  TxnCursorFixture f;
  f.findInsertEraseOverwriteTest();
}

TEST_CASE("TxnCursor/findCreateConflictTest", "")
{
  TxnCursorFixture f;
  f.findCreateConflictTest();
}

TEST_CASE("TxnCursor/moveFirstTest", "")
{
  TxnCursorFixture f;
  f.moveFirstTest();
}

TEST_CASE("TxnCursor/moveFirstInEmptyTreeTest", "")
{
  TxnCursorFixture f;
  f.moveFirstInEmptyTreeTest();
}

TEST_CASE("TxnCursor/moveNextWithNilCursorTest", "")
{
  TxnCursorFixture f;
  f.moveNextWithNilCursorTest();
}

TEST_CASE("TxnCursor/moveNextTest", "")
{
  TxnCursorFixture f;
  f.moveNextTest();
}

TEST_CASE("TxnCursor/moveNextAfterEndTest", "")
{
  TxnCursorFixture f;
  f.moveNextAfterEndTest();
}

TEST_CASE("TxnCursor/moveNextSkipEraseTest", "")
{
  TxnCursorFixture f;
  f.moveNextSkipEraseTest();
}

TEST_CASE("TxnCursor/moveNextSkipEraseInNodeTest", "")
{
  TxnCursorFixture f;
  f.moveNextSkipEraseInNodeTest();
}

TEST_CASE("TxnCursor/moveLastTest", "")
{
  TxnCursorFixture f;
  f.moveLastTest();
}

TEST_CASE("TxnCursor/moveLastInEmptyTreeTest", "")
{
  TxnCursorFixture f;
  f.moveLastInEmptyTreeTest();
}

TEST_CASE("TxnCursor/movePrevWithNilCursorTest", "")
{
  TxnCursorFixture f;
  f.movePrevWithNilCursorTest();
}

TEST_CASE("TxnCursor/movePrevTest", "")
{
  TxnCursorFixture f;
  f.movePrevTest();
}

TEST_CASE("TxnCursor/movePrevAfterEndTest", "")
{
  TxnCursorFixture f;
  f.movePrevAfterEndTest();
}

TEST_CASE("TxnCursor/movePrevSkipEraseTest", "")
{
  TxnCursorFixture f;
  f.movePrevSkipEraseTest();
}

TEST_CASE("TxnCursor/movePrevSkipEraseInNodeTest", "")
{
  TxnCursorFixture f;
  f.movePrevSkipEraseInNodeTest();
}

TEST_CASE("TxnCursor/insertKeysTest", "")
{
  TxnCursorFixture f;
  f.insertKeysTest();
}

TEST_CASE("TxnCursor/negativeInsertKeysTest", "")
{
  TxnCursorFixture f;
  f.negativeInsertKeysTest();
}

TEST_CASE("TxnCursor/insertOverwriteKeysTest", "")
{
  TxnCursorFixture f;
  f.insertOverwriteKeysTest();
}

TEST_CASE("TxnCursor/insertCreateConflictTest", "")
{
  TxnCursorFixture f;
  f.insertCreateConflictTest();
}

TEST_CASE("TxnCursor/overwriteRecordsTest", "")
{
  TxnCursorFixture f;
  f.overwriteRecordsTest();
}

TEST_CASE("TxnCursor/overwriteRecordsNilCursorTest", "")
{
  TxnCursorFixture f;
  f.overwriteRecordsNilCursorTest();
}

TEST_CASE("TxnCursor/approxMatchTest", "")
{
  TxnCursorFixture f;
  f.approxMatchTest();
}

} // namespace hamsterdb
