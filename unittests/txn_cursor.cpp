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

#include "4context/context.h"
#include "4cursor/cursor_local.h"
#include "4db/db_local.h"
#include "4env/env_local.h"
#include "4txn/txn_cursor.h"

#include "fixture.hpp"

namespace upscaledb {

struct TxnCursorFixture : BaseFixture {
  ups_cursor_t *cursor;
  ScopedPtr<Context> context;

  TxnCursorFixture() {
    require_create(UPS_ENABLE_TRANSACTIONS, nullptr,
                    UPS_ENABLE_DUPLICATE_KEYS, nullptr);
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    context.reset(new Context(lenv(), 0, 0));
  }

  ~TxnCursorFixture() {
    context->changeset.clear();
    close();
  }

  TxnNode *create_transaction_node(ups_key_t *key) {
    bool node_created;
    return ldb()->txn_index->store(key, &node_created);
  }

  void cursorIsNilTest() {
    TxnCursor cursor(nullptr);

    REQUIRE(true == cursor.is_nil());
    cursor.set_to_nil();
    REQUIRE(true == cursor.is_nil());
  }

  void getKeyFromCoupledCursorTest() {
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_key_t k = {0};
    ups_record_t record = {0};

    TxnProxy txnp(env);
    TxnNode *node = create_transaction_node(&key);
    TxnOperation *op = node->append(txnp.ltxn(), 0,
                TxnOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != nullptr);

    TxnCursor c((LocalCursor *)cursor);
    c.state_.coupled_op = op;
    c.copy_coupled_key(&k);
    REQUIRE(k.size == key.size);
    REQUIRE(0 == ::memcmp(k.data, key.data, key.size));
    c.set_to_nil();
  }

  void getKeyFromCoupledCursorUserAllocTest() {
    ups_record_t record = {0};
    ups_key_t key = ups_make_key((void *)"hello", 5);

    char buffer[1024] = {0};
    ups_key_t k = ups_make_key(&buffer[0], 0);
    k.flags = UPS_KEY_USER_ALLOC;

    TxnProxy txnp(env);
    TxnNode *node = create_transaction_node(&key);
    TxnOperation *op = node->append(txnp.ltxn(), 0,
                TxnOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != nullptr);

    TxnCursor c((LocalCursor *)cursor);
    c.state_.coupled_op = op;

    c.copy_coupled_key(&k);
    REQUIRE(k.size == key.size);
    REQUIRE(0 == ::memcmp(k.data, key.data, key.size));

    c.set_to_nil();
  }

  void getKeyFromCoupledCursorEmptyKeyTest() {
    ups_key_t k = {0};
    ups_key_t key = {0};
    ups_record_t record = {0};

    TxnProxy txnp(env);
    TxnNode *node = create_transaction_node(&key);
    TxnOperation *op = node->append(txnp.ltxn(), 0,
                TxnOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != nullptr);

    TxnCursor c((LocalCursor *)cursor);
    c.state_.coupled_op = op;
    c.copy_coupled_key(&k);
    REQUIRE(k.size == key.size);
    REQUIRE((void *)0 == k.data);

    c.set_to_nil();
  }

  void getKeyFromNilCursorTest() {
    ups_key_t key = ups_make_key((void *)"hello", 5);
    ups_key_t k = {0};
    ups_record_t record = {0};

    TxnProxy txnp(env);
    TxnNode *node = create_transaction_node(&key);
    TxnOperation *op = node->append(txnp.ltxn(), 0,
                TxnOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != nullptr);

    TxnCursor c((LocalCursor *)cursor);

    REQUIRE_CATCH(c.copy_coupled_key(&k), UPS_CURSOR_IS_NIL);
    c.set_to_nil();
  }

  void getRecordFromCoupledCursorTest() {
    ups_key_t key = {0};
    ups_record_t r = {0};
    ups_record_t record = ups_make_record((void *)"hello", 5);

    TxnProxy txnp(env);
    TxnNode *node = create_transaction_node(&key);
    TxnOperation *op = node->append(txnp.ltxn(), 0,
                TxnOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != nullptr);

    TxnCursor c((LocalCursor *)cursor);
    c.state_.coupled_op = op;

    c.copy_coupled_record(&r);
    REQUIRE(r.size == record.size);
    REQUIRE(0 == memcmp(r.data, record.data, record.size));

    c.set_to_nil();
  }

  void getRecordFromCoupledCursorUserAllocTest() {
    ups_key_t key = {0};
    ups_record_t r = {0};
    ups_record_t record = ups_make_record((void *)"hello", 5);

    char buffer[1024] = {0};
    r.data = &buffer[0];
    r.flags = UPS_RECORD_USER_ALLOC;

    TxnProxy txnp(env);
    TxnNode *node = create_transaction_node(&key);
    TxnOperation *op = node->append(txnp.ltxn(), 0,
                TxnOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != nullptr);

    TxnCursor c((LocalCursor *)cursor);
    c.state_.coupled_op = op;

    c.copy_coupled_record(&r);
    REQUIRE(r.size == record.size);
    REQUIRE(0 == ::memcmp(r.data, record.data, record.size));

    c.set_to_nil();
  }

  void getRecordFromCoupledCursorEmptyRecordTest() {
    ups_key_t key = {0};
    ups_record_t record = {0};
    ups_record_t r = {0};

    TxnProxy txnp(env);
    TxnNode *node = create_transaction_node(&key);
    TxnOperation *op = node->append(txnp.ltxn(), 0,
                TxnOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != nullptr);

    TxnCursor c((LocalCursor *)cursor);
    c.state_.coupled_op = op;

    c.copy_coupled_record(&r);
    REQUIRE(r.size == record.size);
    REQUIRE((void *)0 == r.data);

    c.set_to_nil();
  }

  void getRecordFromNilCursorTest() {
    ups_key_t key = {0};
    ups_record_t record = {0};
    ups_record_t r = {0};

    TxnProxy txnp(env);
    TxnNode *node = create_transaction_node(&key);
    TxnOperation *op = node->append(txnp.ltxn(), 0,
                TxnOperation::kInsertDuplicate, 55, &key, &record);
    REQUIRE(op != nullptr);

    TxnCursor c((LocalCursor *)cursor);
    REQUIRE_CATCH(c.copy_coupled_record(&r), UPS_CURSOR_IS_NIL);
    c.set_to_nil();
  }

  ups_status_t insert(ups_txn_t *txn, const char *key, const char *record = 0,
                  uint32_t flags = 0) {
    ups_key_t k = {0};
    if (key) {
      k.data = (void *)key;
      k.size = ::strlen(key) + 1;
    }
    ups_record_t r = {0};
    if (record) {
      r.data = (void *)record;
      r.size = sizeof(record);
    }
    return ups_db_insert(db, txn, &k, &r, flags);
  }

  ups_status_t insertCursor(ups_cursor_t *cursor, const char *key,
                  const char *record = 0, uint32_t flags = 0) {
    ups_key_t k = {0};
    if (key) {
      k.data = (void *)key;
      k.size = ::strlen(key) + 1;
    }
    ups_record_t r = {0};
    if (record) {
      r.data = (void *)record;
      r.size = ::strlen(record) + 1;
    }
    return ups_cursor_insert(cursor, &k, &r, flags);
  }

  ups_status_t erase(ups_txn_t *txn, const char *key) {
    ups_key_t k = {0};
    if (key) {
      k.data = (void *)key;
      k.size = ::strlen(key) + 1;
    }
    return ups_db_erase(db, txn, &k, 0);
  }

  TxnCursor *txn_cursor(ups_cursor_t *cursor) {
    return &((LocalCursor *)cursor)->txn_cursor;
  }

  ups_status_t findCursor(ups_cursor_t *c, const char *key,
                  const char *record = 0) {
    TxnCursor *cursor = txn_cursor(c);
    ups_key_t k = {0};
    if (key) {
      k.data = (void *)key;
      k.size = ::strlen(key) + 1;
    }
    ups_status_t st = cursor->find(&k, 0);
    if (st)
      return st;
    if (record) {
      ups_record_t r = {0};
      cursor->copy_coupled_record(&r);
      REQUIRE(r.size == strlen(record) + 1);
      REQUIRE(0 == ::memcmp(r.data, record, r.size));
    }
    return 0;
  }

  ups_status_t moveCursor(ups_cursor_t *c, const char *key, uint32_t flags) {
    TxnCursor *cursor = txn_cursor(c);
    ups_key_t k = {0};
    ups_status_t st = cursor->move(flags);
    if (st)
      return st;
    cursor->copy_coupled_key(&k);
    if (key) {
      if (::strcmp((char *)k.data, key))
        return UPS_INTERNAL_ERROR;
    }
    else {
      if (k.size != 0)
        return UPS_INTERNAL_ERROR;
    }
    return 0;
  }

  void findInsertEraseTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert two different keys, delete the first one 
    REQUIRE(0 == insert(txnp.txn, "key1"));
    REQUIRE(0 == erase(txnp.txn, "key1"));
    REQUIRE(0 == insert(txnp.txn, "key2"));

    // find the first key - fails 
    REQUIRE(UPS_KEY_ERASED_IN_TXN == findCursor(cursor, "key1"));

    // insert it again 
    REQUIRE(0 == insert(txnp.txn, "key1"));
    REQUIRE(0 == findCursor(cursor, "key1"));

    // find second key 
    REQUIRE(0 == findCursor(cursor, "key2"));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void findInsertEraseOverwriteTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert a key and overwrite it twice 
    REQUIRE(0 == insert(txnp.txn, "key1", "rec1"));
    REQUIRE(0 == insert(txnp.txn, "key1", "rec2", UPS_OVERWRITE));
    REQUIRE(0 == insert(txnp.txn, "key1", "rec3", UPS_OVERWRITE));

    // find the first key 
    REQUIRE(0 == findCursor(cursor, "key1"));

    // erase it, then insert it again 
    REQUIRE(0 == erase(txnp.txn, "key1"));
    REQUIRE(0 == insert(txnp.txn, "key1", "rec4", UPS_OVERWRITE));
    REQUIRE(0 == findCursor(cursor, "key1"));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void findInsertTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert two different keys 
    REQUIRE(0 == insert(txnp.txn, "key1"));
    REQUIRE(0 == insert(txnp.txn, "key2"));

    // find the first key 
    REQUIRE(0 == findCursor(cursor, "key1"));

    // now the cursor is coupled to this key 
    TxnCursor *txnc = txn_cursor(cursor);
    REQUIRE(!txnc->is_nil());
    TxnOperation *op = txnc->get_coupled_op();
    ups_key_t *key = op->node->key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == ::strcmp((char *)key->data, "key1"));

    // now the key is coupled; find second key 
    REQUIRE(0 == findCursor(cursor, "key2"));

    // and the cursor is still coupled 
    REQUIRE(!txnc->is_nil());
    op = txnc->get_coupled_op();
    key = op->node->key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == ::strcmp((char *)key->data, "key2"));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void moveFirstTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert a few different keys 
    REQUIRE(0 == insert(txnp.txn, "key1"));
    REQUIRE(0 == insert(txnp.txn, "key2"));
    REQUIRE(0 == insert(txnp.txn, "key3"));

    // find the first key (with a nil cursor) 
    REQUIRE(0 == moveCursor(cursor, "key1", UPS_CURSOR_FIRST));

    // now the cursor is coupled to this key 
    TxnCursor *txnc = txn_cursor(cursor);
    REQUIRE(!txnc->is_nil());
    TxnOperation *op = txnc->get_coupled_op();
    ups_key_t *key = op->node->key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == ::strcmp((char *)key->data, "key1"));

    // do it again with a coupled cursor 
    REQUIRE(0 == moveCursor(cursor, "key1", UPS_CURSOR_FIRST));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void moveFirstInEmptyTreeTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // find the first key 
    REQUIRE(UPS_KEY_NOT_FOUND == moveCursor(cursor, "key1", UPS_CURSOR_FIRST));

    // now the cursor is nil 
    TxnCursor *txnc = txn_cursor(cursor);
    REQUIRE(true == txnc->is_nil());

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void findCreateConflictTest() {
    TxnProxy txnp1(env);
    TxnProxy txnp2(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp1.ltxn();

    // insert a key, then erase it 
    REQUIRE(0 == insert(txnp2.txn, "key1"));
    REQUIRE(UPS_TXN_CONFLICT == findCursor(cursor, "key1"));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void moveNextWithNilCursorTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // make sure that the cursor is nil 
    TxnCursor *txnc = txn_cursor(cursor);
    REQUIRE(true == txnc->is_nil());

    REQUIRE(UPS_CURSOR_IS_NIL == moveCursor(cursor, 0, UPS_CURSOR_NEXT));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void moveNextTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert a few different keys 
    REQUIRE(0 == insert(txnp.txn, "key1"));
    REQUIRE(0 == insert(txnp.txn, "key2"));
    REQUIRE(0 == insert(txnp.txn, "key3"));

    // find the first key 
    REQUIRE(0 == findCursor(cursor, "key1"));

    // move next 
    REQUIRE(0 == moveCursor(cursor, "key2", UPS_CURSOR_NEXT));

    // now the cursor is coupled to this key 
    TxnCursor *txnc = txn_cursor(cursor);
    REQUIRE(!txnc->is_nil());
    TxnOperation *op = txnc->get_coupled_op();
    ups_key_t *key = op->node->key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == ::strcmp((char *)key->data, "key2"));

    // now the key is coupled; move next once more 
    REQUIRE(0 == moveCursor(cursor, "key3", UPS_CURSOR_NEXT));

    // and the cursor is still coupled 
    REQUIRE(!txnc->is_nil());
    op = txnc->get_coupled_op();
    key = op->node->key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == ::strcmp((char *)key->data, "key3"));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void moveNextAfterEndTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert one key 
    REQUIRE(0 == insert(txnp.txn, "key1"));

    // find the first key 
    REQUIRE(0 == findCursor(cursor, "key1"));

    // move next 
    REQUIRE(UPS_KEY_NOT_FOUND == moveCursor(cursor, "key2", UPS_CURSOR_NEXT));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void moveNextSkipEraseTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert/erase keys 
    REQUIRE(0 == insert(txnp.txn, "key1"));
    REQUIRE(0 == insert(txnp.txn, "key2"));
    REQUIRE(0 == erase(txnp.txn, "key2"));
    REQUIRE(0 == insert(txnp.txn, "key3"));

    // find the first key 
    REQUIRE(0 == findCursor(cursor, "key1"));

    // move next 
    REQUIRE(UPS_KEY_ERASED_IN_TXN == moveCursor(cursor, 0, UPS_CURSOR_NEXT));

    // move next 
    REQUIRE(0 == moveCursor(cursor, "key3", UPS_CURSOR_NEXT));

    // reached the end 
    REQUIRE(UPS_KEY_NOT_FOUND == moveCursor(cursor, "key3", UPS_CURSOR_NEXT));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void moveNextSkipEraseInNodeTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert/erase keys 
    REQUIRE(0 == insert(txnp.txn, "key1"));
    REQUIRE(0 == insert(txnp.txn, "key2"));
    REQUIRE(0 == erase(txnp.txn, "key2"));
    REQUIRE(0 == insert(txnp.txn, "key2"));
    REQUIRE(0 == erase(txnp.txn, "key2"));
    REQUIRE(0 == insert(txnp.txn, "key2"));
    REQUIRE(0 == erase(txnp.txn, "key2"));
    REQUIRE(0 == insert(txnp.txn, "key3"));

    // find the first key 
    REQUIRE(0 == findCursor(cursor, "key1"));

    // move next 
    REQUIRE(UPS_KEY_ERASED_IN_TXN == moveCursor(cursor, 0, UPS_CURSOR_NEXT));

    // move next 
    REQUIRE(0 == moveCursor(cursor, "key3", UPS_CURSOR_NEXT));

    // reached the end 
    REQUIRE(UPS_KEY_NOT_FOUND == moveCursor(cursor, "key3", UPS_CURSOR_NEXT));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void moveLastTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert a few different keys 
    REQUIRE(0 == insert(txnp.txn, "key1"));
    REQUIRE(0 == insert(txnp.txn, "key2"));
    REQUIRE(0 == insert(txnp.txn, "key3"));

    // find the last key (with a nil cursor) 
    REQUIRE(0 == moveCursor(cursor, "key3", UPS_CURSOR_LAST));

    // now the cursor is coupled to this key 
    TxnCursor *txnc = txn_cursor(cursor);
    REQUIRE(!txnc->is_nil());
    TxnOperation *op = txnc->get_coupled_op();
    ups_key_t *key = op->node->key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == ::strcmp((char *)key->data, "key3"));

    // do it again with a coupled cursor 
    REQUIRE(0 == moveCursor(cursor, "key3", UPS_CURSOR_LAST));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void moveLastInEmptyTreeTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // find the first key 
    REQUIRE(UPS_KEY_NOT_FOUND == moveCursor(cursor, "key1", UPS_CURSOR_LAST));

    // now the cursor is nil 
    TxnCursor *txnc = txn_cursor(cursor);
    REQUIRE(true == txnc->is_nil());

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void movePrevWithNilCursorTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // make sure that the cursor is nil 
    TxnCursor *txnc = txn_cursor(cursor);
    REQUIRE(true == txnc->is_nil());

    REQUIRE(UPS_CURSOR_IS_NIL == moveCursor(cursor, 0, UPS_CURSOR_PREVIOUS));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void movePrevTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert a few different keys 
    REQUIRE(0 == insert(txnp.txn, "key1"));
    REQUIRE(0 == insert(txnp.txn, "key2"));
    REQUIRE(0 == insert(txnp.txn, "key3"));

    // find the last key 
    REQUIRE(0 == findCursor(cursor, "key3"));

    // move previous 
    REQUIRE(0 == moveCursor(cursor, "key2", UPS_CURSOR_PREVIOUS));

    // now the cursor is coupled to this key 
    TxnCursor *txnc = txn_cursor(cursor);
    REQUIRE(!txnc->is_nil());
    TxnOperation *op = txnc->get_coupled_op();
    ups_key_t *key = op->node->key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == ::strcmp((char *)key->data, "key2"));

    // now the key is coupled; move previous once more 
    REQUIRE(0 == moveCursor(cursor, "key1", UPS_CURSOR_PREVIOUS));

    // and the cursor is still coupled 
    REQUIRE(!txnc->is_nil());
    op = txnc->get_coupled_op();
    key = op->node->key();
    REQUIRE(5 == key->size);
    REQUIRE(0 == ::strcmp((char *)key->data, "key1"));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void movePrevAfterEndTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert one key 
    REQUIRE(0 == insert(txnp.txn, "key1"));

    // find the first key 
    REQUIRE(0 == findCursor(cursor, "key1"));

    // move previous 
    REQUIRE(UPS_KEY_NOT_FOUND ==
          moveCursor(cursor, "key2", UPS_CURSOR_PREVIOUS));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void movePrevSkipEraseTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert/erase keys 
    REQUIRE(0 == insert(txnp.txn, "key1"));
    REQUIRE(0 == insert(txnp.txn, "key2"));
    REQUIRE(0 == erase(txnp.txn, "key2"));
    REQUIRE(0 == insert(txnp.txn, "key3"));

    // find the first key 
    REQUIRE(0 == findCursor(cursor, "key3"));

    // move previous 
    REQUIRE(UPS_KEY_ERASED_IN_TXN ==
          moveCursor(cursor, 0, UPS_CURSOR_PREVIOUS));

    // move previous 
    REQUIRE(0 == moveCursor(cursor, "key1", UPS_CURSOR_PREVIOUS));

    // reached the end 
    REQUIRE(UPS_KEY_NOT_FOUND ==
          moveCursor(cursor, "key1", UPS_CURSOR_PREVIOUS));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void movePrevSkipEraseInNodeTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert/erase keys 
    REQUIRE(0 == insert(txnp.txn, "key1"));
    REQUIRE(0 == insert(txnp.txn, "key2"));
    REQUIRE(0 == erase(txnp.txn, "key2"));
    REQUIRE(0 == insert(txnp.txn, "key2"));
    REQUIRE(0 == erase(txnp.txn, "key2"));
    REQUIRE(0 == insert(txnp.txn, "key2"));
    REQUIRE(0 == erase(txnp.txn, "key2"));
    REQUIRE(0 == insert(txnp.txn, "key3"));

    // find the last key 
    REQUIRE(0 == findCursor(cursor, "key3"));

    // move previous 
    REQUIRE(UPS_KEY_ERASED_IN_TXN ==
          moveCursor(cursor, 0, UPS_CURSOR_PREVIOUS));

    // move previous 
    REQUIRE(0 == moveCursor(cursor, "key1", UPS_CURSOR_PREVIOUS));

    // reached the end 
    REQUIRE(UPS_KEY_NOT_FOUND ==
          moveCursor(cursor, "key1", UPS_CURSOR_PREVIOUS));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  bool cursorIsCoupled(ups_cursor_t *c, const char *k) {
    TxnCursor *cursor = &((LocalCursor *)c)->txn_cursor;
    REQUIRE(!cursor->is_nil());
    TxnOperation *op = cursor->get_coupled_op();
    ups_key_t *key = op->node->key();
    if (::strlen(k) + 1 != key->size)
      return false;
    return ::strcmp((char *)key->data, k) == 0;
  }

  void insertKeysTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert a few different keys 
    REQUIRE(0 == insertCursor(cursor, "key1"));
    REQUIRE(0 == insertCursor(cursor, "key2"));
    REQUIRE(0 == insertCursor(cursor, "key3"));

    // make sure that the keys exist and that the cursor is coupled 
    REQUIRE(0 == findCursor(cursor, "key1"));
    REQUIRE(true == cursorIsCoupled(cursor, "key1"));
    REQUIRE(0 == findCursor(cursor, "key2"));
    REQUIRE(true == cursorIsCoupled(cursor, "key2"));
    REQUIRE(0 == findCursor(cursor, "key3"));
    REQUIRE(true == cursorIsCoupled(cursor, "key3"));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void negativeInsertKeysTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert a key twice - creates a duplicate key 
    REQUIRE(0 == insertCursor(cursor, "key1"));
    REQUIRE(UPS_DUPLICATE_KEY == insertCursor(cursor, "key1"));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void insertOverwriteKeysTest() {
    TxnProxy txnp(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp.ltxn();

    // insert/overwrite keys 
    REQUIRE(0 == insertCursor(cursor, "key1"));
    REQUIRE(0 == insertCursor(cursor, "key1", 0, UPS_OVERWRITE));
    REQUIRE(0 == insertCursor(cursor, "key1", 0, UPS_OVERWRITE));

    // make sure that the key exists and that the cursor is coupled 
    REQUIRE(0 == findCursor(cursor, "key1"));
    REQUIRE(true == cursorIsCoupled(cursor, "key1"));

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void insertCreateConflictTest() {
    TxnProxy txnp1(env);
    TxnProxy txnp2(env);

    // hack the cursor and attach it to the txn 
    ((Cursor *)cursor)->txn = txnp1.ltxn();

    // insert/overwrite keys 
    REQUIRE(0 == insert(txnp2.txn, "key1"));
    REQUIRE(UPS_TXN_CONFLICT == insertCursor(cursor, "key1"));

    // cursor must be nil 
    TxnCursor *txnc = txn_cursor(cursor);
    REQUIRE(true == txnc->is_nil());

    // reset cursor hack 
    ((Cursor *)cursor)->txn = 0;
  }

  void approxMatchTest() {
    ups_parameter_t params[] = {
        {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT64},
        {0, 0}
    };

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, nullptr, 0, params);
    DbProxy dbp(db);

    std::vector<uint8_t> data(1024 * 64);
    for (int i = 0; i < 40; i++) {
      uint64_t k = 10 + i * 13;
      ups_key_t key = ups_make_key(&k, sizeof(k));
      dbp.require_insert(&key, data);
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
  }

  void issue101Test() {
    ups_parameter_t params[] = {
        {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32},
        {0, 0}
    };

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, nullptr, 0, params);
    DbProxy dbp(db);

    for (int i = 0; i < 4; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      ups_record_t record = {0};
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
    }

    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    ups_key_t key;

    REQUIRE(0 == ups_cursor_move(cursor, &key, 0, UPS_CURSOR_LAST));
    REQUIRE(3 == *(int *)key.data);
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_move(cursor, &key, 0, UPS_CURSOR_NEXT));
    REQUIRE(3 == *(int *)key.data);
    REQUIRE(0 == ups_cursor_move(cursor, &key, 0, UPS_CURSOR_PREVIOUS));
    REQUIRE(2 == *(int *)key.data);
  }

  void issue101DuplicatesTest() {
    ups_parameter_t params[] = {
        {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32},
        {0, 0}
    };

    close();
    require_create(UPS_ENABLE_TRANSACTIONS, nullptr,
                    UPS_ENABLE_DUPLICATE_KEYS, nullptr);
    DbProxy dbp(db);

    int i = 0;
    for (; i < 4; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      ups_record_t record = {0};

      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
    }

    i = 3;
    ups_key_t key = ups_make_key(&i, sizeof(i));
    ups_record_t record = {0};
    REQUIRE(0 == ups_db_insert(db, 0, &key, &record, UPS_DUPLICATE));

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    int key_val = -1;

    ups_status_t st;

    REQUIRE(0 == ups_cursor_move(cursor, &key, 0, UPS_CURSOR_LAST));
    REQUIRE(3 == *(int *)key.data);
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_move(cursor, &key, 0, UPS_CURSOR_NEXT));
    REQUIRE(3 == *(int *)key.data);
    REQUIRE(0 == ups_cursor_move(cursor, &key, 0, UPS_CURSOR_PREVIOUS));
    REQUIRE(3 == *(int *)key.data);
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

TEST_CASE("TxnCursor/approxMatchTest", "")
{
  TxnCursorFixture f;
  f.approxMatchTest();
}

TEST_CASE("TxnCursor/issue101Test", "")
{
  TxnCursorFixture f;
  f.issue101Test();
}

TEST_CASE("TxnCursor/issue101DuplicatesTest", "")
{
  TxnCursorFixture f;
  f.issue101DuplicatesTest();
}

} // namespace upscaledb
