/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "1base/error.h"
#include "1os/os.h"
#include "2page/page.h"
#include "4context/context.h"
#include "4cursor/cursor_local.h"
#include "4db/db_local.h"
#include "4env/env_local.h"
#include "4txn/txn.h"
#include "4txn/txn_cursor.h"

namespace upscaledb {

struct TxnCursorFixture {
  ups_cursor_t *m_cursor;
  ups_db_t *m_db;
  ups_env_t *m_env;
  ScopedPtr<Context> m_context;

  TxnCursorFixture()
    : m_cursor(0), m_db(0), m_env(0) {
    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test"),
            UPS_ENABLE_RECOVERY | UPS_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 13, UPS_ENABLE_DUPLICATE_KEYS, 0));
    REQUIRE(0 == ups_cursor_create(&m_cursor, m_db, 0, 0));
    m_context.reset(new Context((LocalEnvironment *)m_env, 0, 0));
  }

  ~TxnCursorFixture() {
    m_context->changeset.clear();
    REQUIRE(0 == ups_cursor_close(m_cursor));
    REQUIRE(0 == ups_db_close(m_db, 0));
    REQUIRE(0 == ups_env_close(m_env, 0));
  }

  TransactionNode *create_transaction_node(ups_key_t *key) {
    LocalDatabase *ldb = (LocalDatabase *)m_db;
    TransactionNode *node = new TransactionNode(ldb, key);
    ldb->txn_index()->store(node);
    return (node);
  }

  void cursorIsNilTest() {
    TransactionCursor cursor((LocalCursor *)0);

    REQUIRE(true == cursor.is_nil());
    cursor.set_to_nil();
    REQUIRE(true == cursor.is_nil());
  }

  void getKeyFromCoupledCursorTest() {
    ups_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ups_key_t k = {0};
    ups_record_t record = {0};
    ups_key_t key = ups_make_key((void *)"hello", 5);

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn,
                0, TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != 0);

    TransactionCursor c((LocalCursor *)m_cursor);
    c.m_coupled_op = op;

    c.copy_coupled_key(&k);
    REQUIRE(k.size == key.size);
    REQUIRE(0 == memcmp(k.data, key.data, key.size));

    c.set_to_nil();
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void getKeyFromCoupledCursorUserAllocTest() {
    ups_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ups_record_t record = {0};
    ups_key_t key = ups_make_key((void *)"hello", 5);

    char buffer[1024] = {0};
    ups_key_t k = ups_make_key(&buffer[0], 0);
    k.flags = UPS_KEY_USER_ALLOC;

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn,
                0, TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != 0);

    TransactionCursor c((LocalCursor *)m_cursor);
    c.m_coupled_op = op;

    c.copy_coupled_key(&k);
    REQUIRE(k.size == key.size);
    REQUIRE(0 == memcmp(k.data, key.data, key.size));

    c.set_to_nil();
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void getKeyFromCoupledCursorEmptyKeyTest() {
    ups_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ups_key_t k = {0};
    ups_key_t key = {0};
    ups_record_t record = {0};

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn,
                0, TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op!=0);

    TransactionCursor c((LocalCursor *)m_cursor);
    c.m_coupled_op = op;

    c.copy_coupled_key(&k);
    REQUIRE(k.size == key.size);
    REQUIRE((void *)0 == k.data);

    c.set_to_nil();
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void getKeyFromNilCursorTest() {
    ups_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ups_key_t k = {0};
    ups_record_t record = {0};
    ups_key_t key = ups_make_key((void *)"hello", 5);

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn, 0,
                TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != 0);

    TransactionCursor c((LocalCursor *)m_cursor);

    REQUIRE_CATCH(c.copy_coupled_key(&k), UPS_CURSOR_IS_NIL);

    c.set_to_nil();
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void getRecordFromCoupledCursorTest() {
    ups_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ups_key_t key = {0};
    ups_record_t r = {0};
    ups_record_t record = {0};
    record.data = (void *)"hello";
    record.size = 5;

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn, 0,
                TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op!=0);

    TransactionCursor c((LocalCursor *)m_cursor);
    c.m_coupled_op = op;

    c.copy_coupled_record(&r);
    REQUIRE(r.size == record.size);
    REQUIRE(0 == memcmp(r.data, record.data, record.size));

    c.set_to_nil();
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void getRecordFromCoupledCursorUserAllocTest() {
    ups_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ups_key_t key = {0};
    ups_record_t r = {0};
    ups_record_t record = {0};
    record.data = (void *)"hello";
    record.size = 5;

    char buffer[1024] = {0};
    r.data = &buffer[0];
    r.flags = UPS_RECORD_USER_ALLOC;

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn, 0,
                TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op!=0);

    TransactionCursor c((LocalCursor *)m_cursor);
    c.m_coupled_op = op;

    c.copy_coupled_record(&r);
    REQUIRE(r.size == record.size);
    REQUIRE(0 == memcmp(r.data, record.data, record.size));

    c.set_to_nil();
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void getRecordFromCoupledCursorEmptyRecordTest() {
    ups_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ups_key_t key = {0};
    ups_record_t record = {0};
    ups_record_t r = {0};

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn, 0,
                TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op!=0);

    TransactionCursor c((LocalCursor *)m_cursor);
    c.m_coupled_op = op;

    c.copy_coupled_record(&r);
    REQUIRE(r.size == record.size);
    REQUIRE((void *)0 == r.data);

    c.set_to_nil();
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void getRecordFromNilCursorTest() {
    ups_txn_t *txn;
    TransactionNode *node;
    TransactionOperation *op;
    ups_key_t key = {0};
    ups_record_t record = {0};
    ups_record_t r = {0};

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    node = create_transaction_node(&key);
    op = node->append((LocalTransaction *)txn, 0,
                TransactionOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op!=0);

    TransactionCursor c((LocalCursor *)m_cursor);

    REQUIRE_CATCH(c.copy_coupled_record(&r), UPS_CURSOR_IS_NIL);

    c.set_to_nil();
    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  ups_status_t insert(ups_txn_t *txn, const char *key, const char *record = 0,
          uint32_t flags = 0) {
    ups_key_t k = {0};
    if (key) {
      k.data = (void *)key;
      k.size = strlen(key) + 1;
    }
    ups_record_t r = {0};
    if (record) {
      r.data = (void *)record;
      r.size = sizeof(record);
    }
    return (ups_db_insert(m_db, txn, &k, &r, flags));
  }

  ups_status_t insertCursor(TransactionCursor *cursor, const char *key,
          const char *record = 0, uint32_t flags = 0) {
    ups_key_t k = {0};
    if (key) {
      k.data = (void *)key;
      k.size = strlen(key) + 1;
    }
    ups_record_t r = {0};
    if (record) {
      r.data = (void *)record;
      r.size = strlen(record) + 1;
    }
    return (cursor->test_insert(&k, &r, flags));
  }

  ups_status_t overwriteCursor(TransactionCursor *cursor, const char *record) {
    ups_record_t r = {0};
    if (record) {
      r.data = (void *)record;
      r.size = strlen(record) + 1;
    }

    m_context->txn = (LocalTransaction *)cursor->get_parent()->get_txn();
    return (cursor->overwrite(m_context.get(), m_context->txn, &r));
  }

  ups_status_t erase(ups_txn_t *txn, const char *key) {
    ups_key_t k = {0};
    if (key) {
      k.data = (void *)key;
      k.size = strlen(key) + 1;
    }
    return (ups_db_erase(m_db, txn, &k, 0));
  }

  ups_status_t findCursor(TransactionCursor *cursor, const char *key,
          const char *record = 0) {
    ups_key_t k = {0};
    if (key) {
      k.data = (void *)key;
      k.size = strlen(key) + 1;
    }
    ups_status_t st = cursor->find(&k, 0);
    if (st)
      return (st);
    if (record) {
      ups_record_t r = {0};
      cursor->copy_coupled_record(&r);
      REQUIRE(r.size == strlen(record) + 1);
      REQUIRE(0 == memcmp(r.data, record, r.size));
    }
    return (0);
  }

  ups_status_t moveCursor(TransactionCursor *cursor, const char *key,
          uint32_t flags) {
    ups_key_t k = {0};
    ups_status_t st = cursor->move(flags);
    if (st)
      return (st);
    cursor->copy_coupled_key(&k);
    if (key) {
      if (strcmp((char *)k.data, key))
        return (UPS_INTERNAL_ERROR);
    }
    else {
      if (k.size != 0)
        return (UPS_INTERNAL_ERROR);
    }
    return (UPS_SUCCESS);
  }

  void findInsertEraseTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert two different keys, delete the first one */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == erase(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));

    /* find the first key - fails */
    REQUIRE(UPS_KEY_ERASED_IN_TXN == findCursor(cursor, "key1"));

    /* insert it again */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* find second key */
    REQUIRE(0 == findCursor(cursor, "key2"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void findInsertEraseOverwriteTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert a key and overwrite it twice */
    REQUIRE(0 == insert(txn, "key1", "rec1"));
    REQUIRE(0 == insert(txn, "key1", "rec2", UPS_OVERWRITE));
    REQUIRE(0 == insert(txn, "key1", "rec3", UPS_OVERWRITE));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* erase it, then insert it again */
    REQUIRE(0 == erase(txn, "key1"));
    REQUIRE(0 == insert(txn, "key1", "rec4", UPS_OVERWRITE));
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void findInsertTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert two different keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* now the cursor is coupled to this key */
    REQUIRE(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ups_key_t *key = op->get_node()->get_key();
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
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void moveFirstTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert a few different keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the first key (with a nil cursor) */
    REQUIRE(0 == moveCursor(cursor, "key1", UPS_CURSOR_FIRST));

    /* now the cursor is coupled to this key */
    REQUIRE(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ups_key_t *key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key1"));

    /* do it again with a coupled cursor */
    REQUIRE(0 == moveCursor(cursor, "key1", UPS_CURSOR_FIRST));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void moveFirstInEmptyTreeTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* find the first key */
    REQUIRE(UPS_KEY_NOT_FOUND ==
          moveCursor(cursor, "key1", UPS_CURSOR_FIRST));

    /* now the cursor is nil */
    REQUIRE(true == cursor->is_nil());

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void findCreateConflictTest() {
    ups_txn_t *txn, *txn2;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert a key, then erase it */
    REQUIRE(0 == insert(txn2, "key1"));
    REQUIRE(UPS_TXN_CONFLICT == findCursor(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void moveNextWithNilCursorTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* make sure that the cursor is nil */
    REQUIRE(true == cursor->is_nil());

    REQUIRE(UPS_CURSOR_IS_NIL ==
          moveCursor(cursor, 0, UPS_CURSOR_NEXT));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void moveNextTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert a few different keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* move next */
    REQUIRE(0 == moveCursor(cursor, "key2", UPS_CURSOR_NEXT));

    /* now the cursor is coupled to this key */
    REQUIRE(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ups_key_t *key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key2"));

    /* now the key is coupled; move next once more */
    REQUIRE(0 == moveCursor(cursor, "key3", UPS_CURSOR_NEXT));

    /* and the cursor is still coupled */
    REQUIRE(!cursor->is_nil());
    op = cursor->get_coupled_op();
    key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key3"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void moveNextAfterEndTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert one key */
    REQUIRE(0 == insert(txn, "key1"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* move next */
    REQUIRE(UPS_KEY_NOT_FOUND ==
          moveCursor(cursor, "key2", UPS_CURSOR_NEXT));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void moveNextSkipEraseTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert/erase keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == erase(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* move next */
    REQUIRE(UPS_KEY_ERASED_IN_TXN ==
          moveCursor(cursor, 0, UPS_CURSOR_NEXT));

    /* move next */
    REQUIRE(0 == moveCursor(cursor, "key3", UPS_CURSOR_NEXT));

    /* reached the end */
    REQUIRE(UPS_KEY_NOT_FOUND ==
          moveCursor(cursor, "key3", UPS_CURSOR_NEXT));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void moveNextSkipEraseInNodeTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

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
    REQUIRE(UPS_KEY_ERASED_IN_TXN ==
          moveCursor(cursor, 0, UPS_CURSOR_NEXT));

    /* move next */
    REQUIRE(0 == moveCursor(cursor, "key3", UPS_CURSOR_NEXT));

    /* reached the end */
    REQUIRE(UPS_KEY_NOT_FOUND ==
          moveCursor(cursor, "key3", UPS_CURSOR_NEXT));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void moveLastTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert a few different keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the last key (with a nil cursor) */
    REQUIRE(0 == moveCursor(cursor, "key3", UPS_CURSOR_LAST));

    /* now the cursor is coupled to this key */
    REQUIRE(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ups_key_t *key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key3"));

    /* do it again with a coupled cursor */
    REQUIRE(0 == moveCursor(cursor, "key3", UPS_CURSOR_LAST));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void moveLastInEmptyTreeTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* find the first key */
    REQUIRE(UPS_KEY_NOT_FOUND ==
          moveCursor(cursor, "key1", UPS_CURSOR_LAST));

    /* now the cursor is nil */
    REQUIRE(true == cursor->is_nil());

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void movePrevWithNilCursorTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* make sure that the cursor is nil */
    REQUIRE(true == cursor->is_nil());

    REQUIRE(UPS_CURSOR_IS_NIL ==
          moveCursor(cursor, 0, UPS_CURSOR_PREVIOUS));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void movePrevTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert a few different keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the last key */
    REQUIRE(0 == findCursor(cursor, "key3"));

    /* move previous */
    REQUIRE(0 == moveCursor(cursor, "key2", UPS_CURSOR_PREVIOUS));

    /* now the cursor is coupled to this key */
    REQUIRE(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ups_key_t *key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key2"));

    /* now the key is coupled; move previous once more */
    REQUIRE(0 == moveCursor(cursor, "key1", UPS_CURSOR_PREVIOUS));

    /* and the cursor is still coupled */
    REQUIRE(!cursor->is_nil());
    op = cursor->get_coupled_op();
    key = op->get_node()->get_key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == strcmp((char *)key->data, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void movePrevAfterEndTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert one key */
    REQUIRE(0 == insert(txn, "key1"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key1"));

    /* move previous */
    REQUIRE(UPS_KEY_NOT_FOUND ==
          moveCursor(cursor, "key2", UPS_CURSOR_PREVIOUS));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void movePrevSkipEraseTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert/erase keys */
    REQUIRE(0 == insert(txn, "key1"));
    REQUIRE(0 == insert(txn, "key2"));
    REQUIRE(0 == erase(txn, "key2"));
    REQUIRE(0 == insert(txn, "key3"));

    /* find the first key */
    REQUIRE(0 == findCursor(cursor, "key3"));

    /* move previous */
    REQUIRE(UPS_KEY_ERASED_IN_TXN ==
          moveCursor(cursor, 0, UPS_CURSOR_PREVIOUS));

    /* move previous */
    REQUIRE(0 == moveCursor(cursor, "key1", UPS_CURSOR_PREVIOUS));

    /* reached the end */
    REQUIRE(UPS_KEY_NOT_FOUND ==
          moveCursor(cursor, "key1", UPS_CURSOR_PREVIOUS));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void movePrevSkipEraseInNodeTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

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
    REQUIRE(UPS_KEY_ERASED_IN_TXN ==
          moveCursor(cursor, 0, UPS_CURSOR_PREVIOUS));

    /* move previous */
    REQUIRE(0 == moveCursor(cursor, "key1", UPS_CURSOR_PREVIOUS));

    /* reached the end */
    REQUIRE(UPS_KEY_NOT_FOUND ==
          moveCursor(cursor, "key1", UPS_CURSOR_PREVIOUS));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  bool cursorIsCoupled(TransactionCursor *cursor, const char *k) {
    REQUIRE(!cursor->is_nil());
    TransactionOperation *op = cursor->get_coupled_op();
    ups_key_t *key = op->get_node()->get_key();
    if (strlen(k) + 1 != key->size)
      return (false);
    if (strcmp((char *)key->data, k))
      return (false);
    return (true);
  }

  void insertKeysTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

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
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void negativeInsertKeysTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert a key twice - creates a duplicate key */
    REQUIRE(0 == insertCursor(cursor, "key1"));
    REQUIRE(UPS_DUPLICATE_KEY == insertCursor(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void insertOverwriteKeysTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert/overwrite keys */
    REQUIRE(0 == insertCursor(cursor, "key1"));
    REQUIRE(0 == insertCursor(cursor, "key1", 0, UPS_OVERWRITE));
    REQUIRE(0 == insertCursor(cursor, "key1", 0, UPS_OVERWRITE));

    /* make sure that the key exists and that the cursor is coupled */
    REQUIRE(0 == findCursor(cursor, "key1"));
    REQUIRE(true == cursorIsCoupled(cursor, "key1"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void insertCreateConflictTest() {
    ups_txn_t *txn, *txn2;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn2, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    /* insert/overwrite keys */
    REQUIRE(0 == insert(txn2, "key1"));
    REQUIRE(UPS_TXN_CONFLICT == insertCursor(cursor, "key1"));

    /* cursor must be nil */
    REQUIRE(true == cursor->is_nil());

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
    REQUIRE(0 == ups_txn_commit(txn2, 0));
  }

  void overwriteRecordsTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

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
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void overwriteRecordsNilCursorTest() {
    ups_txn_t *txn;

    TransactionCursor *cursor = ((LocalCursor *)m_cursor)->get_txn_cursor();

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    /* hack the cursor and attach it to the txn */
    ((Cursor *)m_cursor)->m_txn = (Transaction *)txn;

    REQUIRE(UPS_CURSOR_IS_NIL == overwriteCursor(cursor, "rec2"));

    /* reset cursor hack */
    ((Cursor *)m_cursor)->m_txn = 0;

    REQUIRE(0 == ups_txn_commit(txn, 0));
  }

  void approxMatchTest() {
    ups_db_t *db;
    ups_parameter_t params[] = {
      {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT64},
      {0, 0}
    };
    REQUIRE(0 == ups_env_create_db(m_env, &db, 33, 0, &params[0]));

    char data[1024 * 64] = {0};
    for (int i = 0; i < 40; i++) {
      uint64_t k = 10 + i * 13;
      ups_key_t key = ups_make_key(&k, sizeof(k));
      ups_record_t record = ups_make_record(data, sizeof(data));
      REQUIRE(ups_db_insert(db, 0, &key, &record, 0) == 0);
    }

    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    {
      uint64_t k = 0;
      ups_key_t key = ups_make_key(&k, sizeof(k));
      ups_record_t record = {0};
      REQUIRE(0 == ups_db_find(db, 0, &key, &record, UPS_FIND_GEQ_MATCH));
      REQUIRE(key.size == 8);
      REQUIRE(*(uint64_t *)key.data == 10);
    }

    {
      uint64_t k = 0;
      ups_key_t key = ups_make_key(&k, sizeof(k));
      ups_record_t record = {0};
      REQUIRE(0 == ups_cursor_find(cursor, &key, &record, UPS_FIND_GEQ_MATCH));
      REQUIRE(key.size == 8);
      REQUIRE(*(uint64_t *)key.data == 10);
    }

    REQUIRE(0 == ups_db_close(db, UPS_AUTO_CLEANUP));
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

} // namespace upscaledb
