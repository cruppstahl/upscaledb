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

#include "3rdparty/catch/catch.hpp"

#include "globals.h"

#include "../src/env.h"
#include "../src/cursor.h"
#include "../src/btree.h"
#include "../src/btree_cursor.h"

using namespace hamsterdb;

static bool
cursor_is_nil(Cursor *c, int what) {
  return (c->is_nil(what));
}

struct BaseCursorFixture {
  ham_cursor_t *m_cursor;
  ham_db_t *m_db;
  ham_env_t *m_env;
  ham_txn_t *m_txn;

  BaseCursorFixture()
    : m_cursor(0), m_db(0), m_env(0), m_txn(0) {
  }

  ~BaseCursorFixture() {
    teardown();
  }

  virtual void setup() {
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            HAM_ENABLE_RECOVERY | HAM_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 13, HAM_ENABLE_DUPLICATES, 0));
    REQUIRE(0 == createCursor(&m_cursor));
  }

  virtual void teardown() {
    if (m_cursor) {
      REQUIRE(0 == ham_cursor_close(m_cursor));
      m_cursor = 0;
    }
    if (m_env) {
      REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    }
  }

  virtual ham_status_t createCursor(ham_cursor_t **p) {
    return (ham_cursor_create(p, m_db, 0, 0));
  }

  void getDuplicateRecordSizeTest() {
    const int MAX = 20;
    ham_key_t key = {0};
    ham_record_t rec = {0};
    ham_cursor_t *c;
    char data[16];

    REQUIRE(0 == ham_cursor_create(&c, m_db, m_txn, 0));

    for (int i = 0; i < MAX; i++) {
      rec.data = data;
      rec.size = i;
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
          ham_cursor_insert(c, &key, &rec, HAM_DUPLICATE));
    }

    for (int i = 0; i < MAX; i++) {
      ham_u64_t size = 0;

      ::memset(&key, 0, sizeof(key));
      REQUIRE(0 ==
          ham_cursor_move(c, &key, &rec,
            i == 0 ? HAM_CURSOR_FIRST : HAM_CURSOR_NEXT));
      REQUIRE(0 ==
          ham_cursor_get_record_size(c, &size));
      REQUIRE(size == rec.size);
    }

    REQUIRE(0 == ham_cursor_close(c));
  }

  void getRecordSizeTest() {
    const int MAX = 20;
    ham_key_t key = {0};
    ham_record_t rec = {0};
    ham_cursor_t *c;
    char data[16];

    REQUIRE(0 == ham_cursor_create(&c, m_db, m_txn, 0));

    for (int i = 0; i < MAX; i++) {
      key.data = data;
      key.size = sizeof(data);
      rec.data = data;
      rec.size = i;
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
          ham_cursor_insert(c, &key, &rec, HAM_DUPLICATE));
    }

    for (int i = 0; i < MAX; i++) {
      ham_u64_t size = 0;

      key.data = data;
      key.size = sizeof(data);
      REQUIRE(0 ==
          ham_cursor_move(c, &key, &rec,
            i == 0 ? HAM_CURSOR_FIRST : HAM_CURSOR_NEXT));
      REQUIRE(0 ==
          ham_cursor_get_record_size(c, &size));
      REQUIRE(size == rec.size);
    }

    REQUIRE(0 == ham_cursor_close(c));
  }

  void insertFindTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    REQUIRE(HAM_DUPLICATE_KEY ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, HAM_OVERWRITE));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key, &rec, 0));
    REQUIRE(1u ==
          ((Cursor *)m_cursor)->get_dupecache_count());
  }

  void insertFindMultipleCursorsTest(void)
  {
    ham_cursor_t *c[5];
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    for (int i = 0; i < 5; i++)
      REQUIRE(0 == createCursor(&c[i]));

    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    for (int i = 0; i < 5; i++) {
      REQUIRE(0 ==
          ham_cursor_find(c[i], &key, 0, 0));
    }

    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key, &rec, 0));
    REQUIRE(0 == strcmp("12345", (char *)key.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec.data));

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 ==
          ham_cursor_move(c[i], &key, &rec, 0));
      REQUIRE(0 == strcmp("12345", (char *)key.data));
      REQUIRE(0 == strcmp("abcde", (char *)rec.data));
      REQUIRE(0 == ham_cursor_close(c[i]));
    }
  }

  void findInEmptyDatabaseTest() {
    ham_key_t key = {0};
    key.data = (void *)"12345";
    key.size = 6;

    /* this looks up a key in an empty database */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_find(m_cursor, &key, 0, 0));
  }

  void nilCursorTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* cursor is nil */

    REQUIRE(HAM_CURSOR_IS_NIL ==
          ham_cursor_move(m_cursor, &key, &rec, 0));

    REQUIRE(HAM_CURSOR_IS_NIL ==
          ham_cursor_overwrite(m_cursor, &rec, 0));

    ham_cursor_t *clone;
    REQUIRE(0 ==
          ham_cursor_clone(m_cursor, &clone));
    REQUIRE(true == cursor_is_nil((Cursor *)m_cursor, 0));
    REQUIRE(true == cursor_is_nil((Cursor *)clone, 0));
    REQUIRE(0 == ham_cursor_close(clone));
  }
};

struct TempTxnCursorFixture : public BaseCursorFixture {
  TempTxnCursorFixture()
    : BaseCursorFixture() {
    setup();
  }

  void cloneCoupledBtreeCursorTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    ham_cursor_t *clone;

    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    REQUIRE(0 ==
          ham_cursor_clone(m_cursor, &clone));

    REQUIRE(false == cursor_is_nil((Cursor *)clone,
            Cursor::CURSOR_BTREE));
    REQUIRE(0 == ham_cursor_close(clone));
  }

  void cloneUncoupledBtreeCursorTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    Cursor *c = (Cursor *)m_cursor;

    ham_cursor_t *clone;

    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    REQUIRE(0 == c->get_btree_cursor()->uncouple());
    REQUIRE(0 ==
          ham_cursor_clone(m_cursor, &clone));

    ham_key_t *k1 = c->get_btree_cursor()->get_uncoupled_key();
    ham_key_t *k2 = ((Cursor *)clone)->get_btree_cursor()->get_uncoupled_key();
    REQUIRE(0 == strcmp((char *)k1->data, (char *)k2->data));
    REQUIRE(k1->size == k2->size);
    REQUIRE(0 == ham_cursor_close(clone));
  }

  void closeCoupledBtreeCursorTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = (void *)"12345";
    key.size =  6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    Cursor *c = (Cursor *)m_cursor;

    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    REQUIRE(0 == c->get_btree_cursor()->uncouple());

    /* will close in teardown() */
  }

  void closeUncoupledBtreeCursorTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* will close in teardown() */
  }
};

TEST_CASE("Cursor-temptxn/insertFindTest", "")
{
  TempTxnCursorFixture f;
  f.insertFindTest();
}

TEST_CASE("Cursor-temptxn/insertFindMultipleCursorsTest", "")
{
  TempTxnCursorFixture f;
  f.insertFindMultipleCursorsTest();
}

TEST_CASE("Cursor-temptxn/findInEmptyDatabaseTest", "")
{
  TempTxnCursorFixture f;
  f.findInEmptyDatabaseTest();
}

TEST_CASE("Cursor-temptxn/nilCursorTest", "")
{
  TempTxnCursorFixture f;
  f.nilCursorTest();
}

TEST_CASE("Cursor-temptxn/cloneCoupledBtreeCursorTest", "")
{
  TempTxnCursorFixture f;
  f.cloneCoupledBtreeCursorTest();
}

TEST_CASE("Cursor-temptxn/cloneUncoupledBtreeCursorTest", "")
{
  TempTxnCursorFixture f;
  f.cloneUncoupledBtreeCursorTest();
}

TEST_CASE("Cursor-temptxn/closeCoupledBtreeCursorTest", "")
{
  TempTxnCursorFixture f;
  f.closeCoupledBtreeCursorTest();
}

TEST_CASE("Cursor-temptxn/closeUncoupledBtreeCursorTest", "")
{
  TempTxnCursorFixture f;
  f.closeUncoupledBtreeCursorTest();
}


struct NoTxnCursorFixture {
  ham_cursor_t *m_cursor;
  ham_db_t *m_db;
  ham_env_t *m_env;
  ham_txn_t *m_txn;

  NoTxnCursorFixture() {
    setup();
  }

  ~NoTxnCursorFixture() {
    if (m_cursor) {
      REQUIRE(0 == ham_cursor_close(m_cursor));
      m_cursor = 0;
    }
    if (m_env) {
      REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
      m_env = 0;
    }
  }

  void setup() {
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 13, HAM_ENABLE_DUPLICATES, 0));
    REQUIRE(0 == createCursor(&m_cursor));
  }

  ham_status_t createCursor(ham_cursor_t **p) {
    return (ham_cursor_create(p, m_db, 0, 0));
  }

  void moveFirstInEmptyDatabaseTest() {
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
  }
};

TEST_CASE("Cursor-notxn/insertFindTest", "")
{
  BaseCursorFixture f;
  f.setup();
  f.insertFindTest();
}

TEST_CASE("Cursor-notxn/insertFindMultipleCursorsTest", "")
{
  BaseCursorFixture f;
  f.setup();
  f.insertFindMultipleCursorsTest();
}

TEST_CASE("Cursor-notxn/findInEmptyDatabaseTest", "")
{
  BaseCursorFixture f;
  f.setup();
  f.findInEmptyDatabaseTest();
}

TEST_CASE("Cursor-notxn/nilCursorTest", "")
{
  BaseCursorFixture f;
  f.setup();
  f.nilCursorTest();
}

TEST_CASE("Cursor-notxn/moveFirstInEmptyDatabaseTest", "")
{
  NoTxnCursorFixture f;
  f.moveFirstInEmptyDatabaseTest();
}

TEST_CASE("Cursor-notxn/getDuplicateRecordSizeTest", "")
{
  BaseCursorFixture f;
  f.setup();
  f.getDuplicateRecordSizeTest();
}

TEST_CASE("Cursor-notxn/getRecordSizeTest", "")
{
  BaseCursorFixture f;
  f.setup();
  f.getRecordSizeTest();
}

struct InMemoryCursorFixture : public BaseCursorFixture {
  InMemoryCursorFixture() {
    setup();
  }

  virtual void setup() {
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
                HAM_IN_MEMORY, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 13, HAM_ENABLE_DUPLICATES, 0));
  }
};

TEST_CASE("Cursor-inmem/getDuplicateRecordSizeTest", "")
{
  InMemoryCursorFixture f;
  f.getDuplicateRecordSizeTest();
}

TEST_CASE("Cursor-inmem/getRecordSizeTest", "")
{
  InMemoryCursorFixture f;
  f.getRecordSizeTest();
}


struct LongTxnCursorFixture : public BaseCursorFixture {
  LongTxnCursorFixture() {
    setup();
  }

  virtual void setup() {
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            HAM_ENABLE_RECOVERY | HAM_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 13,
            HAM_ENABLE_DUPLICATES | HAM_ENABLE_EXTENDED_KEYS, 0));
    REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));
    REQUIRE(0 == createCursor(&m_cursor));
  }

  virtual ham_status_t createCursor(ham_cursor_t **p) {
    return (ham_cursor_create(p, m_db, m_txn, 0));
  }

  void findInEmptyTransactionTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* this looks up a key in an empty Transaction but with the btree */
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == strcmp("12345", (char *)key.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec.data));
  }

  void findInBtreeOverwrittenInTxnTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;
    rec2.data = (void *)"22222";
    rec2.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* overwrite it in the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec2, HAM_OVERWRITE));

    /* retrieve key and compare record */
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, &rec, 0));
    REQUIRE(0 == strcmp("12345", (char *)key.data));
    REQUIRE(0 == strcmp("22222", (char *)rec.data));
  }

  void findInTxnOverwrittenInTxnTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;
    rec2.data = (void *)"22222";
    rec2.size = 6;

    /* insert a key into the txn */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* overwrite it in the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec2, HAM_OVERWRITE));

    /* retrieve key and compare record */
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, &rec, 0));
    REQUIRE(0 == strcmp("12345", (char *)key.data));
    REQUIRE(0 == strcmp("22222", (char *)rec.data));
  }

  void eraseInTxnKeyFromBtreeTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* couple the cursor to this key */
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));

    /* erase it in the Transaction */
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* key is now nil */
    REQUIRE(true == cursor_is_nil((Cursor *)m_cursor,
            Cursor::CURSOR_BTREE));

    /* retrieve key - must fail */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_find(m_cursor, &key, 0, 0));
  }

  void eraseInTxnKeyFromTxnTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* erase it in the Transaction */
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* retrieve key - must fail */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_find(m_cursor, &key, 0, 0));
  }

  void eraseInTxnOverwrittenKeyTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* overwrite it in the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec2, HAM_OVERWRITE));

    /* erase it in the Transaction */
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* retrieve key - must fail */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_find(m_cursor, &key, 0, 0));
  }

  void eraseInTxnOverwrittenFindKeyTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    REQUIRE(HAM_CURSOR_IS_NIL ==
          ham_cursor_erase(m_cursor, 0));

    /* insert a key into the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* overwrite it in the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec2, HAM_OVERWRITE));

    /* once more couple the cursor to this key */
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));

    /* erase it in the Transaction */
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* retrieve key - must fail */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_find(m_cursor, &key, 0, 0));
  }

  void overwriteInEmptyTransactionTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;
    rec2.data = (void *)"aaaaa";
    rec2.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* this looks up a key in an empty Transaction but with the btree */
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));

    REQUIRE(0 ==
          ham_cursor_overwrite(m_cursor, &rec2, 0));
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, &rec, 0));

    REQUIRE(0 == strcmp("12345", (char *)key.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec.data));
  }

  void overwriteInTransactionTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;
    rec2.data = (void *)"aaaaa";
    rec2.size = 6;


    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    REQUIRE(0 ==
          ham_cursor_overwrite(m_cursor, &rec2, 0));
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, &rec, 0));

    REQUIRE(0 == strcmp("12345", (char *)key.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec.data));
  }

  void cloneCoupledTxnCursorTest() {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    ham_cursor_t *clone;

    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    REQUIRE(0 ==
          ham_cursor_clone(m_cursor, &clone));

    Cursor *c = (Cursor *)m_cursor;
    Cursor *cl = (Cursor *)clone;

    REQUIRE(false ==
        (((Cursor *)clone)->get_btree_cursor()->is_nil()));
    REQUIRE(2u == ((Transaction *)m_txn)->get_cursor_refcount());
    REQUIRE(c->get_txn_cursor()->get_coupled_op() ==
        cl->get_txn_cursor()->get_coupled_op());
    REQUIRE(0 == ham_cursor_close(clone));
    REQUIRE(1u == ((Transaction *)m_txn)->get_cursor_refcount());

  }

  void closeCoupledTxnCursorTest()
  {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* will be closed in teardown() */

  }

  void moveFirstInEmptyTransactionTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("12345", (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));
  }

  void moveFirstInEmptyTransactionExtendedKeyTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    const char *ext = "123456789012345678901234567890";
    key.data = (void *)ext;
    key.size = 31;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp(ext, (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));
  }

  void moveFirstInTransactionTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("12345", (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));
  }

  void moveFirstInTransactionExtendedKeyTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    const char *ext = "123456789012345678901234567890";
    key.data = (void *)ext;
    key.size = 31;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp(ext, (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));
  }

  void moveFirstIdenticalTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* insert the same key into the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, HAM_OVERWRITE));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("12345", (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));

    /* make sure that the cursor is coupled to the txn-op */
    Cursor *c=(Cursor *)m_cursor;
    REQUIRE(c->is_coupled_to_txnop());
  }

  void moveFirstSmallerInTransactionTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a large key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"22222";
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* insert a smaller key into the Transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"xyzab";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("xyzab", (char *)rec2.data));
  }

  void moveFirstSmallerInTransactionExtendedKeyTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    const char *ext1 = "111111111111111111111111111111";
    const char *ext2 = "222222222222222222222222222222";
    key.size = 31;
    rec.size = 6;

    /* insert a large key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)ext2;
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* insert a smaller key into the Transaction */
    key.data = (void *)ext1;
    rec.data = (void *)"xyzab";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp(ext1, (char *)key2.data));
    REQUIRE(0 == strcmp("xyzab", (char *)rec2.data));
  }

  void moveFirstSmallerInBtreeTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a small key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* insert a greater key into the Transaction */
    key.data = (void *)"22222";
    rec.data = (void *)"xyzab";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));
  }

  void moveFirstSmallerInBtreeExtendedKeyTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    const char *ext1 = "111111111111111111111111111111";
    const char *ext2 = "222222222222222222222222222222";
    key.size = 31;
    rec.size = 6;

    /* insert a small key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)ext1;
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* insert a greater key into the Transaction */
    key.data = (void *)ext2;
    rec.data = (void *)"xyzab";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp(ext1, (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));
  }

  void moveFirstErasedInTxnTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* erase it */
    key.data = (void *)"11111";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* this moves the cursor to the first item, but it was erased
     * and therefore this fails */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
  }

  void moveFirstErasedInTxnExtendedKeyTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    const char *ext1 = "111111111111111111111111111111";
    key.size = 31;
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)ext1;
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* erase it */
    key.data = (void *)ext1;
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* this moves the cursor to the first item, but it was erased
     * and therefore this fails */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));

    /* we have to manually clear the changeset, otherwise ham_db_close will
     * fail. The changeset was filled in be->insert(0, but this is an
     * internal function which will not clear it. All other functions fail
     * and therefore do not touch the changeset. */
    ((Environment *)m_env)->get_changeset().clear();
  }

  void moveFirstErasedInsertedInTxnTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* erase it */
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* re-insert it */
    rec.data = (void *)"10101";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("10101", (char *)rec2.data));
  }

  void moveFirstSmallerInBtreeErasedInTxnTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a small key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* insert a greater key into the Transaction */
    key.data = (void *)"22222";
    rec.data = (void *)"xyzab";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* erase the smaller item */
    key.data = (void *)"11111";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* this moves the cursor to the second item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("xyzab", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  void moveLastInEmptyTransactionTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("12345", (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));
  }

  void moveLastInEmptyTransactionExtendedKeyTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    const char *ext = "123456789012345678901234567890";
    key.data = (void *)ext;
    key.size = 31;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp(ext, (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));
  }

  void moveLastInTransactionTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("12345", (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));
  }

  void moveLastInTransactionExtendedKeyTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    const char *ext = "123456789012345678901234567890";
    key.data = (void *)ext;
    key.size = 31;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp(ext, (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));
  }

  void moveLastIdenticalTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.data = (void *)"12345";
    key.size = 6;
    rec.data = (void *)"abcde";
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* insert the same key into the Transaction */
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, HAM_OVERWRITE));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("12345", (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));

    /* make sure that the cursor is coupled to the txn-op */
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
  }

  void moveLastSmallerInTransactionTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a large key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"22222";
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* insert a smaller key into the Transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"xyzab";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));
  }

  void moveLastSmallerInTransactionExtendedKeyTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    const char *ext1 = "111111111111111111111111111111";
    const char *ext2 = "222222222222222222222222222222";
    key.size = 31;
    rec.size = 6;

    /* insert a large key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)ext2;
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* insert a smaller key into the Transaction */
    key.data = (void *)ext1;
    rec.data = (void *)"xyzab";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp(ext2, (char *)key2.data));
    REQUIRE(0 == strcmp("abcde", (char *)rec2.data));
  }

  void moveLastSmallerInBtreeTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a small key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* insert a greater key into the Transaction */
    key.data = (void *)"22222";
    rec.data = (void *)"xyzab";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("xyzab", (char *)rec2.data));
  }

  void moveLastSmallerInBtreeExtendedKeyTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    const char *ext1 = "111111111111111111111111111111";
    const char *ext2 = "222222222222222222222222222222";
    key.size = 31;
    rec.size = 6;

    /* insert a small key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)ext1;
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* insert a greater key into the Transaction */
    key.data = (void *)ext2;
    rec.data = (void *)"xyzab";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp(ext2, (char *)key2.data));
    REQUIRE(0 == strcmp("xyzab", (char *)rec2.data));
  }

  void moveLastErasedInTxnTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* erase it */
    key.data = (void *)"11111";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* this moves the cursor to the last item, but it was erased
     * and therefore this fails */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
  }

  void moveLastErasedInTxnExtendedKeyTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    const char *ext1 = "111111111111111111111111111111";
    key.size = 31;
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)ext1;
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* erase it */
    key.data = (void *)ext1;
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* this moves the cursor to the last item, but it was erased
     * and therefore this fails */
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));

    /* we have to manually clear the changeset, otherwise ham_db_close will
     * fail. The changeset was filled in be->insert(but this is an
     * internal function which will not clear it. All other functions fail
     * and therefore do not touch the changeset. */
    ((Environment *)m_env)->get_changeset().clear();
  }

  void moveLastErasedInsertedInTxnTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* erase it */
    key.data = (void *)"11111";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* re-insert it */
    rec.data = (void *)"10101";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("10101", (char *)rec2.data));
  }

  void moveLastSmallerInBtreeErasedInTxnTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a small key into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* insert a greater key into the Transaction */
    key.data = (void *)"22222";
    rec.data = (void *)"xyzab";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* erase the smaller item */
    key.data = (void *)"11111";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* this moves the cursor to the second item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("xyzab", (char *)rec2.data));
  }

  void moveNextInEmptyTransactionTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  void moveNextInEmptyBtreeTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  void moveNextSmallerInTransactionTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a "small" key into the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    /* and a "greater" one in the btree */
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  void moveNextSmallerInBtreeTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a "small" key into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* and a "large" one in the txn */
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  void moveNextSmallerInTransactionSequenceTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few "small" keys into the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    /* and a few "large" keys in the btree */
    key.data = (void *)"44444";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"55555";
    rec.data = (void *)"eeeee";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"66666";
    rec.data = (void *)"fffff";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("44444", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("55555", (char *)key2.data));
    REQUIRE(0 == strcmp("eeeee", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("66666", (char *)key2.data));
    REQUIRE(0 == strcmp("fffff", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  void moveNextSmallerInBtreeSequenceTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few "small" keys into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* and a few "large" keys in the transaction */
    key.data = (void *)"44444";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"55555";
    rec.data = (void *)"eeeee";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"66666";
    rec.data = (void *)"fffff";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("44444", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("55555", (char *)key2.data));
    REQUIRE(0 == strcmp("eeeee", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("66666", (char *)key2.data));
    REQUIRE(0 == strcmp("fffff", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  void moveNextOverErasedItemTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few "small" keys into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* erase the one in the middle */
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_db_erase(m_db, m_txn, &key, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  void moveNextOverIdenticalItemsTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* overwrite the same keys in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  void moveBtreeThenNextOverIdenticalItemsTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    /* insert a few keys into the btree */
    key.data = (void *)"00000";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* skip the first key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_btree());
    REQUIRE(0 == strcmp("00000", (char *)key2.data));
    REQUIRE(0 == strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  void moveTxnThenNextOverIdenticalItemsTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    key.data = (void *)"00000";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* skip the first key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("00000", (char *)key2.data));
    REQUIRE(0 == strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  void moveNextOverIdenticalItemsThenBtreeTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"99999";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* overwrite all keys but the last */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_btree());
    REQUIRE(0 == strcmp("99999", (char *)key2.data));
    REQUIRE(0 == strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  void moveNextOverIdenticalItemsThenTxnTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"99999";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, 0));
    /* skip the first key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_FIRST));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("99999", (char *)key2.data));
    REQUIRE(0 == strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_NEXT));
  }

  ham_status_t insertBtree(const char *key, const char *rec,
            ham_u32_t flags = 0) {
    ham_key_t k = {0};
    k.data = (void *)key;
    k.size = strlen(key) + 1;
    ham_record_t r = {0};
    r.data = (void *)rec;
    r.size = rec ? strlen(rec) + 1 : 0;

    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    return (be->insert(0, &k, &r, flags));
  }

  ham_status_t insertTxn(const char *key, const char *rec,
            ham_u32_t flags = 0, ham_cursor_t *cursor = 0) {
    ham_key_t k = {0};
    k.data = (void *)key;
    k.size = strlen(key) + 1;
    ham_record_t r={0};
    r.data = (void *)rec;
    r.size = rec ? strlen(rec) + 1 : 0;

    if (cursor)
      return (ham_cursor_insert(cursor, &k, &r, flags));
    else
      return (ham_db_insert(m_db, m_txn, &k, &r, flags));
  }

  ham_status_t eraseTxn(const char *key) {
    ham_key_t k = {0};
    k.data = (void *)key;
    k.size = strlen(key) + 1;

    return (ham_db_erase(m_db, m_txn, &k, 0));
  }

#define BTREE 1
#define TXN   2
  ham_status_t compare(const char *key, const char *rec, int where) {
    ham_key_t k = {0};
    ham_record_t r = {0};
    ham_status_t st;

    st = ham_cursor_move(m_cursor, &k, &r, HAM_CURSOR_NEXT);
    if (st)
      return (st);
    if (strcmp(key, (char *)k.data))
      return (HAM_INTERNAL_ERROR);
    if (strcmp(rec, (char *)r.data))
      return (HAM_INTERNAL_ERROR);
    if (where == BTREE) {
      if (((Cursor *)m_cursor)->is_coupled_to_txnop())
        return (HAM_INTERNAL_ERROR);
    }
    else {
      if (((Cursor *)m_cursor)->is_coupled_to_btree())
        return (HAM_INTERNAL_ERROR);
    }
    return (0);
  }

  ham_status_t comparePrev(const char *key, const char *rec, int where) {
    ham_key_t k = {0};
    ham_record_t r = {0};
    ham_status_t st;

    st=ham_cursor_move(m_cursor, &k, &r, HAM_CURSOR_PREVIOUS);
    if (st)
      return (st);
    if (strcmp(key, (char *)k.data))
      return (HAM_INTERNAL_ERROR);
    if (strcmp(rec, (char *)r.data))
      return (HAM_INTERNAL_ERROR);
    if (where==BTREE) {
      if (((Cursor *)m_cursor)->is_coupled_to_txnop())
        return (HAM_INTERNAL_ERROR);
    }
    else {
      if (((Cursor *)m_cursor)->is_coupled_to_btree())
        return (HAM_INTERNAL_ERROR);
    }
    return (0);
  }

  void moveNextOverSequencesOfIdenticalItemsTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11113", "aaaaa", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11114", "aaaab"));
    REQUIRE(0 == insertTxn  ("11115", "aaaac"));
    REQUIRE(0 == insertBtree("11116", "aaaaa"));
    REQUIRE(0 == insertBtree("11117", "aaaab"));
    REQUIRE(0 == insertBtree("11118", "aaaac"));
    REQUIRE(0 == insertTxn  ("11116", "bbbba", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11117", "bbbbb", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11118", "bbbbc", HAM_OVERWRITE));

    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaab", BTREE));
    REQUIRE(0 == compare  ("11113", "aaaaa", TXN));
    REQUIRE(0 == compare  ("11114", "aaaab", TXN));
    REQUIRE(0 == compare  ("11115", "aaaac", TXN));
    REQUIRE(0 == compare  ("11116", "bbbba", TXN));
    REQUIRE(0 == compare  ("11117", "bbbbb", TXN));
    REQUIRE(0 == compare  ("11118", "bbbbc", TXN));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, 0));
  }

  void moveNextWhileInsertingBtreeTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertBtree("11116", "aaaaa"));
    REQUIRE(0 == insertBtree("11117", "aaaab"));
    REQUIRE(0 == insertBtree("11118", "aaaac"));

    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaab", BTREE));
    REQUIRE(0 == compare  ("11113", "aaaac", BTREE));
    REQUIRE(0 == insertBtree("11114", "aaaax"));
    REQUIRE(0 == compare  ("11114", "aaaax", BTREE));
    REQUIRE(0 == insertBtree("00001", "aaaax"));
    REQUIRE(0 == insertBtree("00002", "aaaax"));
    REQUIRE(0 == compare  ("11116", "aaaaa", BTREE));
    REQUIRE(0 == insertBtree("22222", "aaaax"));
    REQUIRE(0 == compare  ("11117", "aaaab", BTREE));
    REQUIRE(0 == compare  ("11118", "aaaac", BTREE));
    REQUIRE(0 == compare  ("22222", "aaaax", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, 0));
  }

  void moveNextWhileInsertingTransactionTest() {
    REQUIRE(0 == insertTxn("11111", "aaaaa"));
    REQUIRE(0 == insertTxn("11112", "aaaab"));
    REQUIRE(0 == insertTxn("11113", "aaaac"));
    REQUIRE(0 == insertTxn("11116", "aaaaa"));
    REQUIRE(0 == insertTxn("11117", "aaaab"));
    REQUIRE(0 == insertTxn("11118", "aaaac"));

    REQUIRE(0 == compare  ("11111", "aaaaa", TXN));
    REQUIRE(0 == compare  ("11112", "aaaab", TXN));
    REQUIRE(0 == compare  ("11113", "aaaac", TXN));
    REQUIRE(0 == insertTxn("11114", "aaaax"));
    REQUIRE(0 == compare  ("11114", "aaaax", TXN));
    REQUIRE(0 == insertTxn("00001", "aaaax"));
    REQUIRE(0 == insertTxn("00002", "aaaax"));
    REQUIRE(0 == compare  ("11116", "aaaaa", TXN));
    REQUIRE(0 == insertTxn("22222", "aaaax"));
    REQUIRE(0 == compare  ("11117", "aaaab", TXN));
    REQUIRE(0 == compare  ("11118", "aaaac", TXN));
    REQUIRE(0 == compare  ("22222", "aaaax", TXN));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, 0));
  }

  void moveNextWhileInsertingMixedTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11112", "aaaaa", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11117", "aaaab"));
    REQUIRE(0 == insertTxn  ("11118", "aaaac"));
    REQUIRE(0 == insertBtree("11119", "aaaac"));

    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaaa", TXN));
    REQUIRE(0 == insertTxn  ("11113", "xxxxx", HAM_OVERWRITE));
    REQUIRE(0 == compare  ("11113", "xxxxx", TXN));
    REQUIRE(0 == compare  ("11117", "aaaab", TXN));
    REQUIRE(0 == compare  ("11118", "aaaac", TXN));
    REQUIRE(0 == compare  ("11119", "aaaac", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, 0));
  }

  void moveNextWhileErasingTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11114", "aaaad"));
    REQUIRE(0 == insertTxn  ("11115", "aaaae"));
    REQUIRE(0 == insertTxn  ("11116", "aaaaf"));

    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaab", BTREE));
    REQUIRE(0 == eraseTxn   ("11112"));
    REQUIRE(true == cursor_is_nil((Cursor *)m_cursor, 0));
    REQUIRE(true == ((Cursor *)m_cursor)->is_first_use());
    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11113", "aaaac", BTREE));
    REQUIRE(0 == eraseTxn   ("11114"));
    REQUIRE(0 == compare  ("11115", "aaaae", TXN));
    REQUIRE(0 == compare  ("11116", "aaaaf", TXN));
    REQUIRE(0 == eraseTxn   ("11116"));
    REQUIRE(true == cursor_is_nil((Cursor *)m_cursor, 0));
  }

  void movePreviousInEmptyTransactionTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
  }

  void movePreviousInEmptyBtreeTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
  }

  void movePreviousSmallerInTransactionTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a "small" key into the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    /* and a "large" one in the btree */
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
  }

  void movePreviousSmallerInBtreeTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a "small" key into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* and a "large" one in the txn */
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
  }

  void movePreviousSmallerInTransactionSequenceTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few "small" keys into the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    /* and a few "large" keys in the btree */
    key.data = (void *)"44444";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"55555";
    rec.data = (void *)"eeeee";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"66666";
    rec.data = (void *)"fffff";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("66666", (char *)key2.data));
    REQUIRE(0 == strcmp("fffff", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("55555", (char *)key2.data));
    REQUIRE(0 == strcmp("eeeee", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("44444", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
  }

  void movePreviousSmallerInBtreeSequenceTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few "small" keys into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* and a few "large" keys in the transaction */
    key.data = (void *)"44444";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"55555";
    rec.data = (void *)"eeeee";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"66666";
    rec.data = (void *)"fffff";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("66666", (char *)key2.data));
    REQUIRE(0 == strcmp("fffff", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("55555", (char *)key2.data));
    REQUIRE(0 == strcmp("eeeee", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("44444", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
  }

  void movePreviousOverErasedItemTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few "small" keys into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* erase the one in the middle */
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_db_erase(m_db, m_txn, &key, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
  }

  void movePreviousOverIdenticalItemsTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* overwrite the same keys in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
  }

  void moveBtreeThenPreviousOverIdenticalItemsTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    /* insert a few keys into the btree */
    key.data = (void *)"00000";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* skip the first key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_btree());
    REQUIRE(0 == strcmp("00000", (char *)key2.data));
    REQUIRE(0 == strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
  }

  void moveTxnThenPreviousOverIdenticalItemsTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    key.data = (void *)"00000";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == ham_db_insert(m_db, m_txn, &key, &rec, 0));
    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* skip the first key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("00000", (char *)key2.data));
    REQUIRE(0 == strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
  }

  void movePreviousOverIdenticalItemsThenBtreeTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"99999";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    /* skip the last key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_btree());
    REQUIRE(0 == strcmp("99999", (char *)key2.data));
    REQUIRE(0 == strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
  }

  void movePreviousOverIdenticalItemsThenTxnTest() {
    ham_key_t key = {0}, key2 = {0};
    ham_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == be->insert(0, &key, &rec, 0));
    key.data = (void *)"99999";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, 0));
    /* skip the first key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ham_db_insert(m_db, m_txn, &key, &rec, HAM_OVERWRITE));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_LAST));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("99999", (char *)key2.data));
    REQUIRE(0 == strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("33333", (char *)key2.data));
    REQUIRE(0 == strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("22222", (char *)key2.data));
    REQUIRE(0 == strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
    REQUIRE(((Cursor *)m_cursor)->is_coupled_to_txnop());
    REQUIRE(0 == strcmp("11111", (char *)key2.data));
    REQUIRE(0 == strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(m_cursor, &key2, &rec2, HAM_CURSOR_PREVIOUS));
  }

  void movePreviousOverSequencesOfIdenticalItemsTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11113", "aaaaa", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11114", "aaaab"));
    REQUIRE(0 == insertTxn  ("11115", "aaaac"));
    REQUIRE(0 == insertBtree("11116", "aaaaa"));
    REQUIRE(0 == insertBtree("11117", "aaaab"));
    REQUIRE(0 == insertBtree("11118", "aaaac"));
    REQUIRE(0 == insertTxn  ("11116", "bbbba", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11117", "bbbbb", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11118", "bbbbc", HAM_OVERWRITE));

    REQUIRE(0 == comparePrev("11118", "bbbbc", TXN));
    REQUIRE(0 == comparePrev("11117", "bbbbb", TXN));
    REQUIRE(0 == comparePrev("11116", "bbbba", TXN));
    REQUIRE(0 == comparePrev("11115", "aaaac", TXN));
    REQUIRE(0 == comparePrev("11114", "aaaab", TXN));
    REQUIRE(0 == comparePrev("11113", "aaaaa", TXN));
    REQUIRE(0 == comparePrev("11112", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == comparePrev(0, 0, 0));
  }

  void movePreviousWhileInsertingBtreeTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertBtree("11116", "aaaaa"));
    REQUIRE(0 == insertBtree("11117", "aaaab"));
    REQUIRE(0 == insertBtree("11118", "aaaac"));

    REQUIRE(0 == comparePrev("11118", "aaaac", BTREE));
    REQUIRE(0 == comparePrev("11117", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11116", "aaaaa", BTREE));
    REQUIRE(0 == insertBtree("11114", "aaaax"));
    REQUIRE(0 == comparePrev("11114", "aaaax", BTREE));
    REQUIRE(0 == comparePrev("11113", "aaaac", BTREE));
    REQUIRE(0 == comparePrev("11112", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(0 == insertBtree("00000", "aaaax"));
    REQUIRE(0 == comparePrev("00000", "aaaax", BTREE));
    REQUIRE(0 == insertBtree("00001", "aaaax"));
    REQUIRE(0 == insertBtree("00002", "aaaax"));
    REQUIRE(HAM_KEY_NOT_FOUND == comparePrev(0, 0, 0));
  }

  void movePreviousWhileInsertingTransactionTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("11112", "aaaab"));
    REQUIRE(0 == insertTxn  ("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11116", "aaaaa"));
    REQUIRE(0 == insertTxn  ("11117", "aaaab"));
    REQUIRE(0 == insertTxn  ("11118", "aaaac"));

    REQUIRE(0 == comparePrev("11118", "aaaac", TXN));
    REQUIRE(0 == comparePrev("11117", "aaaab", TXN));
    REQUIRE(0 == comparePrev("11116", "aaaaa", TXN));
    REQUIRE(0 == insertTxn  ("11114", "aaaax"));
    REQUIRE(0 == comparePrev("11114", "aaaax", TXN));
    REQUIRE(0 == comparePrev("11113", "aaaac", TXN));
    REQUIRE(0 == comparePrev("11112", "aaaab", TXN));
    REQUIRE(0 == comparePrev("11111", "aaaaa", TXN));
    REQUIRE(0 == insertTxn  ("00000", "aaaax"));
    REQUIRE(0 == comparePrev("00000", "aaaax", TXN));

    REQUIRE(0 == insertTxn  ("00001", "aaaax"));
    REQUIRE(0 == insertTxn  ("00002", "aaaax"));
    REQUIRE(HAM_KEY_NOT_FOUND == comparePrev(0, 0, 0));
  }

  void movePreviousWhileInsertingMixedTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11112", "aaaaa", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11117", "aaaab"));
    REQUIRE(0 == insertTxn  ("11118", "aaaac"));
    REQUIRE(0 == insertBtree("11119", "aaaac"));

    REQUIRE(0 == comparePrev("11119", "aaaac", BTREE));
    REQUIRE(0 == comparePrev("11118", "aaaac", TXN));
    REQUIRE(0 == comparePrev("11117", "aaaab", TXN));
    REQUIRE(0 == insertTxn  ("11113", "xxxxx", HAM_OVERWRITE));
    REQUIRE(0 == comparePrev("11113", "xxxxx", TXN));
    REQUIRE(0 == comparePrev("11112", "aaaaa", TXN));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == comparePrev(0, 0, 0));
  }

  void switchDirectionsInBtreeTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertBtree("11114", "aaaad"));
    REQUIRE(0 == insertBtree("11115", "aaaae"));
    REQUIRE(0 == insertBtree("11116", "aaaaf"));
    REQUIRE(0 == insertBtree("11116", "aaaag", HAM_OVERWRITE));
    REQUIRE(0 == insertBtree("11117", "aaaah"));
    REQUIRE(0 == insertBtree("11118", "aaaai"));
    REQUIRE(0 == insertBtree("11119", "aaaaj"));

    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaab", BTREE));
    REQUIRE(0 == compare  ("11113", "aaaac", BTREE));
    REQUIRE(0 == compare  ("11114", "aaaad", BTREE));
    REQUIRE(0 == comparePrev("11113", "aaaac", BTREE));
    REQUIRE(0 == comparePrev("11112", "aaaab", BTREE));
    REQUIRE(0 == compare  ("11113", "aaaac", BTREE));
    REQUIRE(0 == compare  ("11114", "aaaad", BTREE));
    REQUIRE(0 == compare  ("11115", "aaaae", BTREE));
    REQUIRE(0 == compare  ("11116", "aaaag", BTREE));
    REQUIRE(0 == compare  ("11117", "aaaah", BTREE));
    REQUIRE(0 == compare  ("11118", "aaaai", BTREE));
    REQUIRE(0 == compare  ("11119", "aaaaj", BTREE));
    REQUIRE(0 == comparePrev("11118", "aaaai", BTREE));
    REQUIRE(0 == comparePrev("11117", "aaaah", BTREE));
    REQUIRE(0 == comparePrev("11116", "aaaag", BTREE));
  }

  void switchDirectionsInTransactionTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("11112", "aaaab"));
    REQUIRE(0 == insertTxn  ("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11114", "aaaad"));
    REQUIRE(0 == insertTxn  ("11115", "aaaae"));
    REQUIRE(0 == insertTxn  ("11116", "aaaaf"));
    REQUIRE(0 == insertTxn  ("11116", "aaaag", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11117", "aaaah"));
    REQUIRE(0 == insertTxn  ("11118", "aaaai"));
    REQUIRE(0 == insertTxn  ("11119", "aaaaj"));

    REQUIRE(0 == compare  ("11111", "aaaaa", TXN));
    REQUIRE(0 == compare  ("11112", "aaaab", TXN));
    REQUIRE(0 == comparePrev("11111", "aaaaa", TXN));
    REQUIRE(0 == compare  ("11112", "aaaab", TXN));
    REQUIRE(0 == compare  ("11113", "aaaac", TXN));
    REQUIRE(0 == compare  ("11114", "aaaad", TXN));
    REQUIRE(0 == comparePrev("11113", "aaaac", TXN));
    REQUIRE(0 == comparePrev("11112", "aaaab", TXN));
    REQUIRE(0 == compare  ("11113", "aaaac", TXN));
    REQUIRE(0 == compare  ("11114", "aaaad", TXN));
    REQUIRE(0 == compare  ("11115", "aaaae", TXN));
    REQUIRE(0 == compare  ("11116", "aaaag", TXN));
    REQUIRE(0 == compare  ("11117", "aaaah", TXN));
    REQUIRE(0 == compare  ("11118", "aaaai", TXN));
    REQUIRE(0 == compare  ("11119", "aaaaj", TXN));
    REQUIRE(0 == comparePrev("11118", "aaaai", TXN));
    REQUIRE(0 == comparePrev("11117", "aaaah", TXN));
    REQUIRE(0 == comparePrev("11116", "aaaag", TXN));
  }

  void switchDirectionsMixedStartInBtreeTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11114", "aaaad"));
    REQUIRE(0 == insertBtree("11115", "aaaae"));
    REQUIRE(0 == insertTxn  ("11116", "aaaaf"));
    REQUIRE(0 == insertTxn  ("11116", "aaaag", HAM_OVERWRITE));
    REQUIRE(0 == insertBtree("11117", "aaaah"));
    REQUIRE(0 == insertTxn  ("11118", "aaaai"));
    REQUIRE(0 == insertBtree("11119", "aaaaj"));
    REQUIRE(0 == insertTxn  ("11119", "aaaak", HAM_OVERWRITE));

    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaab", TXN));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaab", TXN));
    REQUIRE(0 == compare  ("11113", "aaaac", BTREE));
    REQUIRE(0 == compare  ("11114", "aaaad", TXN));
    REQUIRE(0 == comparePrev("11113", "aaaac", BTREE));
    REQUIRE(0 == comparePrev("11112", "aaaab", TXN));
    REQUIRE(0 == compare  ("11113", "aaaac", BTREE));
    REQUIRE(0 == compare  ("11114", "aaaad", TXN));
    REQUIRE(0 == compare  ("11115", "aaaae", BTREE));
    REQUIRE(0 == compare  ("11116", "aaaag", TXN));
    REQUIRE(0 == compare  ("11117", "aaaah", BTREE));
    REQUIRE(0 == compare  ("11118", "aaaai", TXN));
    REQUIRE(0 == compare  ("11119", "aaaak", TXN));
    REQUIRE(0 == comparePrev("11118", "aaaai", TXN));
    REQUIRE(0 == comparePrev("11117", "aaaah", BTREE));
    REQUIRE(0 == comparePrev("11116", "aaaag", TXN));
  }

  void switchDirectionsMixedStartInTxnTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertTxn  ("11113", "aaaac"));
    REQUIRE(0 == insertBtree("11114", "aaaad"));
    REQUIRE(0 == insertTxn  ("11115", "aaaae"));
    REQUIRE(0 == insertBtree("11116", "aaaaf"));
    REQUIRE(0 == insertTxn  ("11116", "aaaag", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11117", "aaaah"));
    REQUIRE(0 == insertTxn  ("11118", "aaaai"));
    REQUIRE(0 == insertBtree("11119", "aaaaj"));

    REQUIRE(0 == compare  ("11111", "aaaaa", TXN));
    REQUIRE(0 == compare  ("11112", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", TXN));
    REQUIRE(0 == compare  ("11112", "aaaab", BTREE));
    REQUIRE(0 == compare  ("11113", "aaaac", TXN));
    REQUIRE(0 == compare  ("11114", "aaaad", BTREE));
    REQUIRE(0 == comparePrev("11113", "aaaac", TXN));
    REQUIRE(0 == comparePrev("11112", "aaaab", BTREE));
    REQUIRE(0 == compare  ("11113", "aaaac", TXN));
    REQUIRE(0 == compare  ("11114", "aaaad", BTREE));
    REQUIRE(0 == compare  ("11115", "aaaae", TXN));
    REQUIRE(0 == compare  ("11116", "aaaag", TXN));
    REQUIRE(0 == compare  ("11117", "aaaah", TXN));
    REQUIRE(0 == compare  ("11118", "aaaai", TXN));
    REQUIRE(0 == compare  ("11119", "aaaaj", BTREE));
    REQUIRE(0 == comparePrev("11118", "aaaai", TXN));
    REQUIRE(0 == comparePrev("11117", "aaaah", TXN));
    REQUIRE(0 == comparePrev("11116", "aaaag", TXN));
  }

  void switchDirectionsMixedSequenceTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertBtree("11114", "aaaad"));
    REQUIRE(0 == insertTxn  ("11113", "aaaae", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11114", "aaaaf", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11115", "aaaag", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11116", "aaaah"));
    REQUIRE(0 == insertTxn  ("11117", "aaaai"));
    REQUIRE(0 == insertBtree("11118", "aaaaj"));
    REQUIRE(0 == insertBtree("11119", "aaaak"));
    REQUIRE(0 == insertBtree("11120", "aaaal"));
    REQUIRE(0 == insertBtree("11121", "aaaam"));
    REQUIRE(0 == insertTxn  ("11120", "aaaan", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11121", "aaaao", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11122", "aaaap"));

    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaab", BTREE));
    REQUIRE(0 == compare  ("11113", "aaaae", TXN));
    REQUIRE(0 == compare  ("11114", "aaaaf", TXN));
    REQUIRE(0 == comparePrev("11113", "aaaae", TXN));
    REQUIRE(0 == comparePrev("11112", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == comparePrev(0, 0, BTREE));
    ((Cursor *)m_cursor)->set_to_nil(0);
    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaab", BTREE));
    REQUIRE(0 == compare  ("11113", "aaaae", TXN));
    REQUIRE(0 == compare  ("11114", "aaaaf", TXN));
    REQUIRE(0 == compare  ("11115", "aaaag", TXN));
    REQUIRE(0 == compare  ("11116", "aaaah", TXN));
    REQUIRE(0 == compare  ("11117", "aaaai", TXN));
    REQUIRE(0 == compare  ("11118", "aaaaj", BTREE));
    REQUIRE(0 == compare  ("11119", "aaaak", BTREE));
    REQUIRE(0 == compare  ("11120", "aaaan", TXN));
    REQUIRE(0 == compare  ("11121", "aaaao", TXN));
    REQUIRE(0 == compare  ("11122", "aaaap", TXN));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, BTREE));
    ((Cursor *)m_cursor)->set_to_nil(0);
    REQUIRE(0 == comparePrev("11122", "aaaap", TXN));
    REQUIRE(0 == comparePrev("11121", "aaaao", TXN));
    REQUIRE(0 == comparePrev("11120", "aaaan", TXN));
    REQUIRE(0 == comparePrev("11119", "aaaak", BTREE));
    REQUIRE(0 == comparePrev("11118", "aaaaj", BTREE));
    REQUIRE(0 == comparePrev("11117", "aaaai", TXN));
    REQUIRE(0 == comparePrev("11116", "aaaah", TXN));
    REQUIRE(0 == comparePrev("11115", "aaaag", TXN));
    REQUIRE(0 == comparePrev("11114", "aaaaf", TXN));
    REQUIRE(0 == comparePrev("11113", "aaaae", TXN));
    REQUIRE(0 == compare  ("11114", "aaaaf", TXN));
    REQUIRE(0 == compare  ("11115", "aaaag", TXN));
    REQUIRE(0 == compare  ("11116", "aaaah", TXN));
    REQUIRE(0 == compare  ("11117", "aaaai", TXN));
    REQUIRE(0 == compare  ("11118", "aaaaj", BTREE));
    REQUIRE(0 == compare  ("11119", "aaaak", BTREE));
    REQUIRE(0 == compare  ("11120", "aaaan", TXN));
    REQUIRE(0 == compare  ("11121", "aaaao", TXN));
    REQUIRE(0 == compare  ("11122", "aaaap", TXN));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, BTREE));
  }

  void findTxnThenMoveNextTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertTxn  ("33333", "aaaac"));
    REQUIRE(0 == insertBtree("44444", "aaaad"));
    REQUIRE(0 == insertBtree("55555", "aaaae"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare  ("44444", "aaaad", BTREE));
    REQUIRE(0 == compare  ("55555", "aaaae", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, BTREE));
  }

  void findTxnThenMoveNext2Test() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("44444", "aaaad"));
    REQUIRE(0 == insertBtree("55555", "aaaae"));
    REQUIRE(0 == insertBtree("66666", "aaaaf"));
    REQUIRE(0 == insertTxn  ("77777", "aaaag"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"44444";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare  ("55555", "aaaae", BTREE));
    REQUIRE(0 == compare  ("66666", "aaaaf", BTREE));
    REQUIRE(0 == compare  ("77777", "aaaag", TXN));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, BTREE));
  }

  void findTxnThenMovePreviousTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertTxn  ("33333", "aaaac"));
    REQUIRE(0 == insertBtree("44444", "aaaad"));
    REQUIRE(0 == insertBtree("55555", "aaaae"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == comparePrev("22222", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == comparePrev(0, 0, BTREE));
  }

  void findTxnThenMoveNext3Test() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("33333", "aaaad", HAM_OVERWRITE));
    REQUIRE(0 == insertTxn  ("44444", "aaaae"));
    REQUIRE(0 == insertTxn  ("55555", "aaaaf"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare("44444", "aaaae", TXN));
    REQUIRE(0 == compare("55555", "aaaaf", TXN));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, TXN));
  }

  void findTxnThenMoveNext4Test() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("33333", "aaaad", HAM_OVERWRITE));
    REQUIRE(0 == insertBtree("44444", "aaaae"));
    REQUIRE(0 == insertBtree("55555", "aaaaf"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare("44444", "aaaae", BTREE));
    REQUIRE(0 == compare("55555", "aaaaf", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, TXN));
  }

  void findTxnThenMovePrevious2Test() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("44444", "aaaad"));
    REQUIRE(0 == insertBtree("55555", "aaaae"));
    REQUIRE(0 == insertBtree("66666", "aaaaf"));
    REQUIRE(0 == insertTxn  ("77777", "aaaag"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"44444";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == comparePrev("33333", "aaaac", BTREE));
    REQUIRE(0 == comparePrev("22222", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", TXN));
    REQUIRE(HAM_KEY_NOT_FOUND == comparePrev(0, 0, BTREE));
  }

  void findTxnThenMovePrevious3Test() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("33333", "aaaad", HAM_OVERWRITE));
    REQUIRE(0 == insertBtree("44444", "aaaae"));
    REQUIRE(0 == insertBtree("55555", "aaaaf"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == comparePrev("22222", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == comparePrev(0, 0, TXN));
  }

  void findTxnThenMovePrevious4Test() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("33333", "aaaad", HAM_OVERWRITE));
    REQUIRE(0 == insertBtree("44444", "aaaae"));
    REQUIRE(0 == insertBtree("55555", "aaaaf"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == comparePrev("22222", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == comparePrev(0, 0, TXN));
  }

  void findBtreeThenMoveNextTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("44444", "aaaad"));
    REQUIRE(0 == insertTxn  ("55555", "aaaae"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare  ("44444", "aaaad", TXN));
    REQUIRE(0 == compare  ("55555", "aaaae", TXN));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, TXN));
  }

  void findBtreeThenMovePreviousTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("44444", "aaaad"));
    REQUIRE(0 == insertTxn  ("55555", "aaaae"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == comparePrev("22222", "aaaab", TXN));
    REQUIRE(0 == comparePrev("11111", "aaaaa", TXN));
    REQUIRE(HAM_KEY_NOT_FOUND == comparePrev(0, 0, TXN));
  }

  void findBtreeThenMovePrevious2Test() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("22222", "aaaab"));
    REQUIRE(0 == insertTxn  ("33333", "aaaac"));
    REQUIRE(0 == insertBtree("44444", "aaaad"));
    REQUIRE(0 == insertTxn  ("55555", "aaaae"));
    REQUIRE(0 == insertTxn  ("66666", "aaaaf"));
    REQUIRE(0 == insertBtree("77777", "aaaag"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"44444";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == comparePrev("33333", "aaaac", TXN));
    REQUIRE(0 == comparePrev("22222", "aaaab", TXN));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == comparePrev(0, 0, BTREE));
  }

  void findBtreeThenMoveNext2Test() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("22222", "aaaab"));
    REQUIRE(0 == insertTxn  ("33333", "aaaac"));
    REQUIRE(0 == insertBtree("44444", "aaaad"));
    REQUIRE(0 == insertTxn  ("55555", "aaaae"));
    REQUIRE(0 == insertTxn  ("66666", "aaaaf"));
    REQUIRE(0 == insertBtree("77777", "aaaag"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"44444";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare  ("55555", "aaaae", TXN));
    REQUIRE(0 == compare  ("66666", "aaaaf", TXN));
    REQUIRE(0 == compare  ("77777", "aaaag", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, BTREE));
  }

  void findBtreeThenMoveNext3Test() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("33333", "aaaad", HAM_OVERWRITE));
    REQUIRE(0 == insertBtree("44444", "aaaae"));
    REQUIRE(0 == insertBtree("55555", "aaaaf"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare("44444", "aaaae", BTREE));
    REQUIRE(0 == compare("55555", "aaaaf", BTREE));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, TXN));
  }

  void insertThenMoveNextTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("44444", "aaaad"));
    REQUIRE(0 == insertTxn  ("55555", "aaaae"));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    ham_record_t rec = {0};
    rec.size = 6;
    rec.data = (void *)"33333";
    REQUIRE(0 ==
          ham_cursor_insert(m_cursor, &key, &rec, HAM_OVERWRITE));
    REQUIRE(0 == compare  ("44444", "aaaad", TXN));
    REQUIRE(0 == compare  ("55555", "aaaae", TXN));
    REQUIRE(HAM_KEY_NOT_FOUND == compare(0, 0, TXN));
  }

  void abortWhileCursorActiveTest() {
    REQUIRE(HAM_CURSOR_STILL_OPEN == ham_txn_abort(m_txn, 0));
  }

  void commitWhileCursorActiveTest()
  {
    REQUIRE(HAM_CURSOR_STILL_OPEN == ham_txn_commit(m_txn, 0));
  }

  void eraseKeyWithTwoCursorsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    ham_cursor_t *cursor2;
    REQUIRE(0 ==
          ham_cursor_clone(m_cursor, &cursor2));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ham_cursor_find(cursor2, &key, 0, 0));

    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    REQUIRE(true == cursor_is_nil((Cursor *)m_cursor, 0));
    REQUIRE(true == cursor_is_nil((Cursor *)cursor2, 0));

    REQUIRE(0 == ham_cursor_close(cursor2));
  }

  void eraseKeyWithTwoCursorsOverwriteTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    ham_cursor_t *cursor2;
    REQUIRE(0 ==
          ham_cursor_clone(m_cursor, &cursor2));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    ham_record_t rec = {0};
    rec.size = 6;
    rec.data = (void *)"11111";
    REQUIRE(0 ==
          ham_cursor_insert(cursor2, &key, &rec, HAM_OVERWRITE));

    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    REQUIRE(true == cursor_is_nil((Cursor *)m_cursor, 0));
    REQUIRE(true == cursor_is_nil((Cursor *)cursor2, 0));

    REQUIRE(0 == ham_cursor_close(cursor2));
  }

  void eraseWithThreeCursorsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    ham_cursor_t *cursor2, *cursor3;
    REQUIRE(0 ==
          ham_cursor_create(&cursor2, m_db, m_txn, 0));
    REQUIRE(0 ==
          ham_cursor_create(&cursor3, m_db, m_txn, 0));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"11111";
    ham_record_t rec = {0};
    rec.size = 6;
    rec.data = (void *)"33333";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ham_cursor_insert(cursor2, &key, &rec, HAM_OVERWRITE));
    REQUIRE(0 ==
          ham_cursor_insert(cursor3, &key, &rec, HAM_OVERWRITE));

    REQUIRE(0 ==
          ham_db_erase(m_db, m_txn, &key, 0));
    REQUIRE(true == cursor_is_nil((Cursor *)m_cursor, 0));
    REQUIRE(true == cursor_is_nil((Cursor *)cursor2, 0));
    REQUIRE(true == cursor_is_nil((Cursor *)cursor3, 0));

    REQUIRE(0 == ham_cursor_close(cursor2));
    REQUIRE(0 == ham_cursor_close(cursor3));
  }

  void eraseKeyWithoutCursorsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    ham_cursor_t *cursor2;
    REQUIRE(0 ==
          ham_cursor_clone(m_cursor, &cursor2));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ham_cursor_find(cursor2, &key, 0, 0));

    REQUIRE(HAM_TXN_CONFLICT ==
          ham_db_erase(m_db, 0, &key, 0));
    REQUIRE(0 ==
          ham_db_erase(m_db, m_txn, &key, 0));
    REQUIRE(true == cursor_is_nil((Cursor *)m_cursor, 0));
    REQUIRE(true == cursor_is_nil((Cursor *)cursor2, 0));

    REQUIRE(0 == ham_cursor_close(cursor2));
  }

  void eraseKeyAndFlushTransactionsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));

    /* create a second txn, insert and commit, but do not flush the
     * first one */
    ham_txn_t *txn2;
    REQUIRE(0 == ham_txn_begin(&txn2, m_env, 0, 0, 0));

    ham_cursor_t *cursor2;
    REQUIRE(0 ==
          ham_cursor_create(&cursor2, m_db, txn2, 0));

    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.size = 6;
    key.data = (void *)"11112";
    REQUIRE(0 ==
          ham_cursor_insert(cursor2, &key, &rec, 0));
    REQUIRE(0 ==
          ham_cursor_close(cursor2));

    /* commit the 2nd txn - it will not be flushed because an older
     * txn also was not flushed */
    REQUIRE(0 == ham_txn_commit(txn2, 0));

    /* the other cursor is part of the first transaction; position on
     * the new key */
    REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));

    /* now erase the key */
    REQUIRE(0 ==
          ham_db_erase(m_db, m_txn, &key, 0));

    /* cursor must be nil */
    REQUIRE(true == cursor_is_nil((Cursor *)m_cursor, 0));
  }

  ham_status_t move(const char *key, const char *rec, ham_u32_t flags,
        ham_cursor_t *cursor = 0) {
    ham_key_t k = {0};
    ham_record_t r = {0};
    ham_status_t st;

    if (!cursor)
      cursor = m_cursor;

    st = ham_cursor_move(cursor, &k, &r, flags);
    if (st)
      return (st);
    if (strcmp(key, (char *)k.data))
      return (HAM_INTERNAL_ERROR);
    if (rec)
      if (strcmp(rec, (char *)r.data))
        return (HAM_INTERNAL_ERROR);

    // now verify again, but with flags=0
    if (flags == 0)
      return (0);
    st = ham_cursor_move(cursor, &k, &r, 0);
    if (st)
      return (st);
    if (strcmp(key, (char *)k.data))
      return (HAM_INTERNAL_ERROR);
    if (rec)
      if (strcmp(rec, (char *)r.data))
        return (HAM_INTERNAL_ERROR);
    return (0);
  }

  void moveLastThenInsertNewLastTest() {
    REQUIRE(0 == insertTxn("11111", "bbbbb"));
    REQUIRE(0 == insertTxn("22222", "ccccc"));

    REQUIRE(0 == move("22222", "ccccc", HAM_CURSOR_LAST));
    REQUIRE(0 == move("11111", "bbbbb", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == insertTxn("00000", "aaaaa"));
    REQUIRE(0 == move("00000", "aaaaa", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void moveFirstThenInsertNewFirstTest() {
    REQUIRE(0 == insertTxn("11111", "aaaaa"));
    REQUIRE(0 == insertTxn("22222", "bbbbb"));

    REQUIRE(0 == move("11111", "aaaaa", HAM_CURSOR_FIRST));
    REQUIRE(0 == move("22222", "bbbbb", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == insertTxn("33333", "ccccc"));
    REQUIRE(0 == move("33333", "ccccc", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
  }
};

TEST_CASE("Cursor-longtxn/getDuplicateRecordSizeTest", "")
{
  LongTxnCursorFixture f;
  f.getDuplicateRecordSizeTest();
}

TEST_CASE("Cursor-longtxn/getRecordSizeTest", "")
{
  LongTxnCursorFixture f;
  f.getRecordSizeTest();
}

TEST_CASE("Cursor-longtxn/insertFindTest", "")
{
  LongTxnCursorFixture f;
  f.insertFindTest();
}

TEST_CASE("Cursor-longtxn/insertFindMultipleCursorsTest", "")
{
  LongTxnCursorFixture f;
  f.insertFindMultipleCursorsTest();
}

TEST_CASE("Cursor-longtxn/findInEmptyDatabaseTest", "")
{
  LongTxnCursorFixture f;
  f.findInEmptyDatabaseTest();
}

TEST_CASE("Cursor-longtxn/findInEmptyTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.findInEmptyTransactionTest();
}

TEST_CASE("Cursor-longtxn/findInBtreeOverwrittenInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.findInBtreeOverwrittenInTxnTest();
}

TEST_CASE("Cursor-longtxn/findInTxnOverwrittenInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.findInTxnOverwrittenInTxnTest();
}

TEST_CASE("Cursor-longtxn/eraseInTxnKeyFromBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.eraseInTxnKeyFromBtreeTest();
}

TEST_CASE("Cursor-longtxn/eraseInTxnKeyFromTxnTest", "")
{
  LongTxnCursorFixture f;
  f.eraseInTxnKeyFromTxnTest();
}

TEST_CASE("Cursor-longtxn/eraseInTxnOverwrittenKeyTest", "")
{
  LongTxnCursorFixture f;
  f.eraseInTxnOverwrittenKeyTest();
}

TEST_CASE("Cursor-longtxn/eraseInTxnOverwrittenFindKeyTest", "")
{
  LongTxnCursorFixture f;
  f.eraseInTxnOverwrittenFindKeyTest();
}

TEST_CASE("Cursor-longtxn/overwriteInEmptyTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.overwriteInEmptyTransactionTest();
}

TEST_CASE("Cursor-longtxn/overwriteInTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.overwriteInTransactionTest();
}

TEST_CASE("Cursor-longtxn/cloneCoupledTxnCursorTest", "")
{
  LongTxnCursorFixture f;
  f.cloneCoupledTxnCursorTest();
}

TEST_CASE("Cursor-longtxn/closeCoupledTxnCursorTest", "")
{
  LongTxnCursorFixture f;
  f.closeCoupledTxnCursorTest();
}

TEST_CASE("Cursor-longtxn/moveFirstInEmptyTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstInEmptyTransactionTest();
}

TEST_CASE("Cursor-longtxn/moveFirstInEmptyTransactionExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstInEmptyTransactionExtendedKeyTest();
}

TEST_CASE("Cursor-longtxn/moveFirstInTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstInTransactionTest();
}

TEST_CASE("Cursor-longtxn/moveFirstInTransactionExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstInTransactionExtendedKeyTest();
}

TEST_CASE("Cursor-longtxn/moveFirstIdenticalTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstIdenticalTest();
}

TEST_CASE("Cursor-longtxn/moveFirstSmallerInTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstSmallerInTransactionTest();
}

TEST_CASE("Cursor-longtxn/moveFirstSmallerInTransactionExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstSmallerInTransactionExtendedKeyTest();
}

TEST_CASE("Cursor-longtxn/moveFirstSmallerInBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstSmallerInBtreeTest();
}

TEST_CASE("Cursor-longtxn/moveFirstSmallerInBtreeExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstSmallerInBtreeExtendedKeyTest();
}

TEST_CASE("Cursor-longtxn/moveFirstErasedInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstErasedInTxnTest();
}

TEST_CASE("Cursor-longtxn/moveFirstErasedInTxnExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstErasedInTxnExtendedKeyTest();
}

TEST_CASE("Cursor-longtxn/moveFirstErasedInsertedInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstErasedInsertedInTxnTest();
}

TEST_CASE("Cursor-longtxn/moveFirstSmallerInBtreeErasedInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstSmallerInBtreeErasedInTxnTest();
}

TEST_CASE("Cursor-longtxn/moveLastInEmptyTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastInEmptyTransactionTest();
}

TEST_CASE("Cursor-longtxn/moveLastInEmptyTransactionExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastInEmptyTransactionExtendedKeyTest();
}

TEST_CASE("Cursor-longtxn/moveLastInTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastInTransactionTest();
}

TEST_CASE("Cursor-longtxn/moveLastInTransactionExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastInTransactionExtendedKeyTest();
}

TEST_CASE("Cursor-longtxn/moveLastIdenticalTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastIdenticalTest();
}

TEST_CASE("Cursor-longtxn/moveLastSmallerInTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastSmallerInTransactionTest();
}

TEST_CASE("Cursor-longtxn/moveLastSmallerInTransactionExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastSmallerInTransactionExtendedKeyTest();
}

TEST_CASE("Cursor-longtxn/moveLastSmallerInBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastSmallerInBtreeTest();
}

TEST_CASE("Cursor-longtxn/moveLastSmallerInBtreeExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastSmallerInBtreeExtendedKeyTest();
}

TEST_CASE("Cursor-longtxn/moveLastErasedInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastErasedInTxnTest();
}

TEST_CASE("Cursor-longtxn/moveLastErasedInTxnExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastErasedInTxnExtendedKeyTest();
}

TEST_CASE("Cursor-longtxn/moveLastErasedInsertedInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastErasedInsertedInTxnTest();
}

TEST_CASE("Cursor-longtxn/moveLastSmallerInBtreeErasedInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastSmallerInBtreeErasedInTxnTest();
}

TEST_CASE("Cursor-longtxn/nilCursorTest", "")
{
  LongTxnCursorFixture f;
  f.nilCursorTest();
}

TEST_CASE("Cursor-longtxn/moveNextInEmptyTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextInEmptyTransactionTest();
}

TEST_CASE("Cursor-longtxn/moveNextInEmptyBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextInEmptyBtreeTest();
}

TEST_CASE("Cursor-longtxn/moveNextSmallerInTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextSmallerInTransactionTest();
}

TEST_CASE("Cursor-longtxn/moveNextSmallerInBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextSmallerInBtreeTest();
}

TEST_CASE("Cursor-longtxn/moveNextSmallerInTransactionSequenceTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextSmallerInTransactionSequenceTest();
}

TEST_CASE("Cursor-longtxn/moveNextSmallerInBtreeSequenceTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextSmallerInBtreeSequenceTest();
}

TEST_CASE("Cursor-longtxn/moveNextOverErasedItemTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextOverErasedItemTest();
}

TEST_CASE("Cursor-longtxn/moveNextOverIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextOverIdenticalItemsTest();
}

TEST_CASE("Cursor-longtxn/moveBtreeThenNextOverIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.moveBtreeThenNextOverIdenticalItemsTest();
}

TEST_CASE("Cursor-longtxn/moveTxnThenNextOverIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.moveTxnThenNextOverIdenticalItemsTest();
}

TEST_CASE("Cursor-longtxn/moveNextOverIdenticalItemsThenBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextOverIdenticalItemsThenBtreeTest();
}

TEST_CASE("Cursor-longtxn/moveNextOverIdenticalItemsThenTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextOverIdenticalItemsThenTxnTest();
}

TEST_CASE("Cursor-longtxn/moveNextOverSequencesOfIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextOverSequencesOfIdenticalItemsTest();
}

TEST_CASE("Cursor-longtxn/moveNextWhileInsertingBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextWhileInsertingBtreeTest();
}

TEST_CASE("Cursor-longtxn/moveNextWhileInsertingTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextWhileInsertingTransactionTest();
}

TEST_CASE("Cursor-longtxn/moveNextWhileInsertingMixedTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextWhileInsertingMixedTest();
}

TEST_CASE("Cursor-longtxn/moveNextWhileErasingTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextWhileErasingTest();
}

TEST_CASE("Cursor-longtxn/movePreviousInEmptyTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousInEmptyTransactionTest();
}

TEST_CASE("Cursor-longtxn/movePreviousInEmptyBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousInEmptyBtreeTest();
}

TEST_CASE("Cursor-longtxn/movePreviousSmallerInTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousSmallerInTransactionTest();
}

TEST_CASE("Cursor-longtxn/movePreviousSmallerInBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousSmallerInBtreeTest();
}

TEST_CASE("Cursor-longtxn/movePreviousSmallerInTransactionSequenceTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousSmallerInTransactionSequenceTest();
}

TEST_CASE("Cursor-longtxn/movePreviousSmallerInBtreeSequenceTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousSmallerInBtreeSequenceTest();
}

TEST_CASE("Cursor-longtxn/movePreviousOverErasedItemTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousOverErasedItemTest();
}

TEST_CASE("Cursor-longtxn/movePreviousOverIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousOverIdenticalItemsTest();
}

TEST_CASE("Cursor-longtxn/moveBtreeThenPreviousOverIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.moveBtreeThenPreviousOverIdenticalItemsTest();
}

TEST_CASE("Cursor-longtxn/moveTxnThenPreviousOverIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.moveTxnThenPreviousOverIdenticalItemsTest();
}

TEST_CASE("Cursor-longtxn/movePreviousOverIdenticalItemsThenBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousOverIdenticalItemsThenBtreeTest();
}

TEST_CASE("Cursor-longtxn/movePreviousOverIdenticalItemsThenTxnTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousOverIdenticalItemsThenTxnTest();
}

TEST_CASE("Cursor-longtxn/movePreviousOverSequencesOfIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousOverSequencesOfIdenticalItemsTest();
}

TEST_CASE("Cursor-longtxn/movePreviousWhileInsertingBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousWhileInsertingBtreeTest();
}

TEST_CASE("Cursor-longtxn/movePreviousWhileInsertingTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousWhileInsertingTransactionTest();
}

TEST_CASE("Cursor-longtxn/movePreviousWhileInsertingMixedTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousWhileInsertingMixedTest();
}

TEST_CASE("Cursor-longtxn/switchDirectionsInBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.switchDirectionsInBtreeTest();
}

TEST_CASE("Cursor-longtxn/switchDirectionsInTransactionTest", "")
{
  LongTxnCursorFixture f;
  f.switchDirectionsInTransactionTest();
}

TEST_CASE("Cursor-longtxn/switchDirectionsMixedStartInBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.switchDirectionsMixedStartInBtreeTest();
}

TEST_CASE("Cursor-longtxn/switchDirectionsMixedStartInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.switchDirectionsMixedStartInTxnTest();
}

TEST_CASE("Cursor-longtxn/switchDirectionsMixedSequenceTest", "")
{
  LongTxnCursorFixture f;
  f.switchDirectionsMixedSequenceTest();
}

TEST_CASE("Cursor-longtxn/findTxnThenMoveNextTest", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMoveNextTest();
}

TEST_CASE("Cursor-longtxn/findTxnThenMoveNext2Test", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMoveNext2Test();
}

TEST_CASE("Cursor-longtxn/findTxnThenMoveNext3Test", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMoveNext3Test();
}

TEST_CASE("Cursor-longtxn/findTxnThenMoveNext4Test", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMoveNext4Test();
}

TEST_CASE("Cursor-longtxn/findTxnThenMovePreviousTest", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMovePreviousTest();
}

TEST_CASE("Cursor-longtxn/findTxnThenMovePrevious2Test", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMovePrevious2Test();
}

TEST_CASE("Cursor-longtxn/findTxnThenMovePrevious3Test", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMovePrevious3Test();
}

TEST_CASE("Cursor-longtxn/findTxnThenMovePrevious4Test", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMovePrevious4Test();
}

TEST_CASE("Cursor-longtxn/findBtreeThenMoveNextTest", "")
{
  LongTxnCursorFixture f;
  f.findBtreeThenMoveNextTest();
}

TEST_CASE("Cursor-longtxn/findBtreeThenMoveNext2Test", "")
{
  LongTxnCursorFixture f;
  f.findBtreeThenMoveNext2Test();
}

TEST_CASE("Cursor-longtxn/findBtreeThenMoveNext3Test", "")
{
  LongTxnCursorFixture f;
  f.findBtreeThenMoveNext3Test();
}

TEST_CASE("Cursor-longtxn/findBtreeThenMovePreviousTest", "")
{
  LongTxnCursorFixture f;
  f.findBtreeThenMovePreviousTest();
}

TEST_CASE("Cursor-longtxn/findBtreeThenMovePrevious2Test", "")
{
  LongTxnCursorFixture f;
  f.findBtreeThenMovePrevious2Test();
}

TEST_CASE("Cursor-longtxn/insertThenMoveNextTest", "")
{
  LongTxnCursorFixture f;
  f.insertThenMoveNextTest();
}

TEST_CASE("Cursor-longtxn/abortWhileCursorActiveTest", "")
{
  LongTxnCursorFixture f;
  f.abortWhileCursorActiveTest();
}

TEST_CASE("Cursor-longtxn/commitWhileCursorActiveTest", "")
{
  LongTxnCursorFixture f;
  f.commitWhileCursorActiveTest();
}

TEST_CASE("Cursor-longtxn/eraseKeyWithTwoCursorsTest", "")
{
  LongTxnCursorFixture f;
  f.eraseKeyWithTwoCursorsTest();
}

// TODO why was this removed? FC_REGISTER_TEST(LongTxnCursorTest,
      //eraseKeyWithTwoCursorsOverwriteTest);

TEST_CASE("Cursor-longtxn/eraseWithThreeCursorsTest", "")
{
  LongTxnCursorFixture f;
  f.eraseWithThreeCursorsTest();
}

TEST_CASE("Cursor-longtxn/eraseKeyWithoutCursorsTest", "")
{
  LongTxnCursorFixture f;
  f.eraseKeyWithoutCursorsTest();
}

TEST_CASE("Cursor-longtxn/eraseKeyAndFlushTransactionsTest", "")
{
  LongTxnCursorFixture f;
  f.eraseKeyAndFlushTransactionsTest();
}

TEST_CASE("Cursor-longtxn/moveLastThenInsertNewLastTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastThenInsertNewLastTest();
}

TEST_CASE("Cursor-longtxn/moveFirstThenInsertNewFirstTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstThenInsertNewFirstTest();
}



struct DupeCacheFixture {
  ham_cursor_t *m_cursor;
  ham_db_t *m_db;
  ham_env_t *m_env;

  DupeCacheFixture() {
    REQUIRE(0 ==
            ham_env_create(&m_env, Globals::opath(".test"), 0, 0664, 0));
    REQUIRE(0 ==
            ham_env_create_db(m_env, &m_db, 13, HAM_ENABLE_DUPLICATES, 0));
    REQUIRE(0 == ham_cursor_create(&m_cursor, m_db, 0, 0));
  }

  ~DupeCacheFixture() {
    REQUIRE(0 == ham_cursor_close(m_cursor));
    REQUIRE(0 == ham_db_close(m_db, HAM_TXN_AUTO_COMMIT));
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void createEmptyCloseTest() {
    DupeCache c;
    REQUIRE(0u == c.get_count());
  }

  void appendTest() {
    DupeCache c;
    DupeCacheLine entries[20];
    for (int i = 0; i < 20; i++)
      entries[i].set_btree_dupe_idx(i);

    for (int i = 0; i < 20; i++)
      c.append(entries[i]);
    REQUIRE(20u == c.get_count());

    DupeCacheLine *e = c.get_first_element();
    for (int i = 0; i < 20; i++) {
      REQUIRE((ham_u64_t)i == e->get_btree_dupe_idx());
      e++;
    }
  }

  void insertAtBeginningTest() {
    DupeCache c;

    DupeCacheLine entries[20];
    for (int i = 0; i < 20; i++)
      entries[i].set_btree_dupe_idx(i);

    for (int i = 0; i < 20; i++)
      c.insert(0, entries[i]);
    REQUIRE(20u == c.get_count());

    DupeCacheLine *e = c.get_first_element();
    for (int i = 19, j = 0; i >= 0; i--, j++) {
      REQUIRE((ham_u64_t)i == e->get_btree_dupe_idx());
      e++;
    }
  }

  void insertAtEndTest() {
    DupeCache c;

    DupeCacheLine entries[20];
    for (int i = 0; i < 20; i++)
      entries[i].set_btree_dupe_idx(i);

    for (int i = 0; i < 20; i++)
      c.insert(i, entries[i]);
    REQUIRE(20u == c.get_count());

    DupeCacheLine *e = c.get_first_element();
    for (int i = 0; i < 20; i++) {
      REQUIRE((ham_u64_t)i == e->get_btree_dupe_idx());
      e++;
    }
  }

  void insertMixedTest() {
    DupeCache c;

    DupeCacheLine entries[20];
    for (int i = 0; i < 20; i++)
      entries[i].set_btree_dupe_idx(i);

    int p = 0;
    for (int j = 0; j < 5; j++) {
      for (int i = 0; i < 4; i++) {
        c.insert(j, entries[p++]);
      }
    }
    REQUIRE(20u == c.get_count());

    DupeCacheLine *e = c.get_first_element();
    REQUIRE((ham_u64_t)3 ==  e[ 0].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)7 ==  e[ 1].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)11 == e[ 2].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)15 == e[ 3].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)19 == e[ 4].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)18 == e[ 5].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)17 == e[ 6].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)16 == e[ 7].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)14 == e[ 8].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)13 == e[ 9].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)12 == e[10].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)10 == e[11].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)9 ==  e[12].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)8 ==  e[13].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)6 ==  e[14].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)5 ==  e[15].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)4 ==  e[16].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)2 ==  e[17].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)1 ==  e[18].get_btree_dupe_idx());
    REQUIRE((ham_u64_t)0 ==  e[19].get_btree_dupe_idx());
  }

  void eraseAtBeginningTest() {
    DupeCache c;

    DupeCacheLine entries[20];
    for (int i = 0; i < 20; i++)
      entries[i].set_btree_dupe_idx(i);

    for (int i = 0; i < 20; i++)
      c.append(entries[i]);
    REQUIRE(20u == c.get_count());

    int s = 1;
    for (int i = 19; i >= 0; i--) {
      DupeCacheLine *e = c.get_first_element();
      c.erase(0);
      REQUIRE((unsigned)i == c.get_count());
      for (int j = 0; j < i; j++) {
        REQUIRE((ham_u64_t)(s + j) == e->get_btree_dupe_idx());
        e++;
      }
      s++;
    }

    REQUIRE(0u == c.get_count());
  }

  void eraseAtEndTest() {
    DupeCache c;

    DupeCacheLine entries[20];
    for (int i = 0; i < 20; i++)
      entries[i].set_btree_dupe_idx(i);

    for (int i = 0; i < 20; i++)
      c.append(entries[i]);
    REQUIRE(20u == c.get_count());

    for (int i = 0; i < 20; i++) {
      DupeCacheLine *e = c.get_first_element();
      c.erase(c.get_count() - 1);
      for (int j = 0; j < 20 - i; j++) {
        REQUIRE((ham_u64_t)j == e->get_btree_dupe_idx());
        e++;
      }
    }

    REQUIRE(0u == c.get_count());
  }

  void eraseMixedTest() {
    DupeCache c;

    DupeCacheLine entries[20];
    for (int i = 0; i < 20; i++)
      entries[i].set_btree_dupe_idx(i);

    for (int i = 0; i < 20; i++)
      c.append(entries[i]);
    REQUIRE(20u == c.get_count());

    for (int i = 0; i < 10; i++)
      c.erase(i);

    DupeCacheLine *e = c.get_first_element();
    for (int i = 0; i < 10; i++) {
      REQUIRE((unsigned)(i * 2 + 1) == e->get_btree_dupe_idx());
      e++;
    }

    REQUIRE(10u == c.get_count());
  }
};

TEST_CASE("Cursor-dcache/createEmptyCloseTest", "")
{
  DupeCacheFixture f;
  f.createEmptyCloseTest();
}

TEST_CASE("Cursor-dcache/appendTest", "")
{
  DupeCacheFixture f;
  f.appendTest();
}

TEST_CASE("Cursor-dcache/insertAtBeginningTest", "")
{
  DupeCacheFixture f;
  f.insertAtBeginningTest();
}

TEST_CASE("Cursor-dcache/insertAtEndTest", "")
{
  DupeCacheFixture f;
  f.insertAtEndTest();
}

TEST_CASE("Cursor-dcache/insertMixedTest", "")
{
  DupeCacheFixture f;
  f.insertMixedTest();
}

TEST_CASE("Cursor-dcache/eraseAtBeginningTest", "")
{
  DupeCacheFixture f;
  f.eraseAtBeginningTest();
}

TEST_CASE("Cursor-dcache/eraseAtEndTest", "")
{
  DupeCacheFixture f;
  f.eraseAtEndTest();
}

TEST_CASE("Cursor-dcache/eraseMixedTest", "")
{
  DupeCacheFixture f;
  f.eraseMixedTest();
}

struct DupeCursorFixture {
  ham_cursor_t *m_cursor;
  ham_db_t *m_db;
  ham_env_t *m_env;
  ham_txn_t *m_txn;

  DupeCursorFixture() {
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 13, HAM_ENABLE_DUPLICATES, 0));
    REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&m_cursor, m_db, m_txn, 0));
  }

  ~DupeCursorFixture() {
    teardown();
  }

  void teardown() {
    REQUIRE(0 == ham_cursor_close(m_cursor));
    m_cursor = 0;
    if (m_txn)
      REQUIRE(0 == ham_txn_commit(m_txn, 0));
    REQUIRE(0 == ham_db_close(m_db, HAM_TXN_AUTO_COMMIT));
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  ham_status_t insertBtree(const char *key, const char *rec,
            ham_u32_t flags = 0) {
    ham_key_t k = {0};
    k.data = (void *)key;
    k.size = strlen(key) + 1;
    ham_record_t r = {0};
    r.data = (void *)rec;
    r.size = rec ? strlen(rec) + 1 : 0;

    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    return (be->insert(0, &k, &r, flags));
  }

  ham_status_t eraseTxn(const char *key) {
    ham_key_t k = {0};
    k.data = (void *)key;
    k.size = strlen(key) + 1;

    return (ham_db_erase(m_db, m_txn, &k, 0));
  }

  ham_status_t move(const char *key, const char *rec, ham_u32_t flags,
        ham_cursor_t *cursor = 0) {
    ham_key_t k = {0};
    ham_record_t r = {0};

    if (!cursor)
      cursor=m_cursor;

    ham_status_t st = ham_cursor_move(cursor, &k, &r, flags);
    if (st)
      return (st);
    if (strcmp(key, (char *)k.data))
      return (HAM_INTERNAL_ERROR);
    if (rec)
      if (strcmp(rec, (char *)r.data))
        return (HAM_INTERNAL_ERROR);

    // now verify again, but with flags=0
    if (flags == 0)
      return (0);
    st = ham_cursor_move(cursor, &k, &r, 0);
    if (st)
      return (st);
    if (strcmp(key, (char *)k.data))
      return (HAM_INTERNAL_ERROR);
    if (rec)
      if (strcmp(rec, (char *)r.data))
        return (HAM_INTERNAL_ERROR);
    return (0);
  }

  ham_status_t find(const char *key, const char *rec) {
    ham_key_t k = {0};
    ham_record_t r = {0};
    ham_status_t st = ham_db_find(m_db, m_txn, &k, &r, 0);
    if (st)
      return (st);
    if (strcmp(key, (char *)k.data))
      return (HAM_INTERNAL_ERROR);
    if (strcmp(rec, (char *)r.data))
      return (HAM_INTERNAL_ERROR);

    return (0);
  }

  ham_status_t insertTxn(const char *key, const char *rec,
            ham_u32_t flags = 0) {
    ham_key_t k = {0};
    k.data = (void *)key;
    k.size = strlen(key) + 1;
    ham_record_t r = {0};
    r.data = (void *)rec;
    r.size = rec ? strlen(rec) + 1 : 0;

    return (ham_cursor_insert(m_cursor, &k, &r, flags));
  }

  void simpleBtreeTest() {
    REQUIRE(0 == insertBtree("33333", "aaaaa"));
    REQUIRE(0 == insertBtree("33333", "aaaab", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("33333", "aaaac", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("33333", "aaaad", HAM_DUPLICATE));

    REQUIRE(0 == move     ("33333", "aaaaa", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("33333", "aaaab", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaac", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaad", HAM_CURSOR_NEXT));
    REQUIRE(4u ==
          ((Cursor *)m_cursor)->get_dupecache_count());
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaad", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("33333", "aaaac", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaab", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaaa", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void multipleBtreeTest() {
    REQUIRE(0 == insertBtree("33333", "aaaaa"));
    REQUIRE(0 == insertBtree("33333", "aaaab", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("33333", "aaaac", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11111", "aaaab", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("11111", "aaaac", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("44444", "aaaaa"));
    REQUIRE(0 == insertBtree("44444", "aaaab", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("44444", "aaaac", HAM_DUPLICATE));

    REQUIRE(0 == move     ("11111", "aaaaa", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("11111", "aaaab", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("11111", "aaaac", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaaa", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaab", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaac", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("44444", "aaaaa", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("44444", "aaaab", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("44444", "aaaac", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("44444", "aaaac", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("44444", "aaaab", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("44444", "aaaaa", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaac", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("44444", "aaaaa", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaac", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaab", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaaa", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("11111", "aaaac", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("11111", "aaaab", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("11111", "aaaaa", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void simpleTxnInsertLastTest() {
    REQUIRE(0 == insertTxn  ("33333", "aaaaa"));
    REQUIRE(0 == insertTxn  ("33333", "aaaab", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("33333", "aaaac", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("33333", "aaaad", HAM_DUPLICATE));

    REQUIRE(0 == move     ("33333", "aaaaa", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("33333", "aaaab", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaac", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaad", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaad", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("33333", "aaaac", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaab", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaaa", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void simpleTxnInsertFirstTest() {
    REQUIRE(0 == insertTxn  ("33333", "aaaaa"));
    REQUIRE(0 == insertTxn  ("33333", "aaaab",
          HAM_DUPLICATE|HAM_DUPLICATE_INSERT_FIRST));
    REQUIRE(0 == insertTxn  ("33333", "aaaac",
          HAM_DUPLICATE|HAM_DUPLICATE_INSERT_FIRST));
    REQUIRE(0 == insertTxn  ("33333", "aaaad",
          HAM_DUPLICATE|HAM_DUPLICATE_INSERT_FIRST));

    REQUIRE(0 == move     ("33333", "aaaad", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("33333", "aaaac", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaab", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaaa", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("33333", "aaaaa", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("33333", "aaaab", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaac", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("33333", "aaaad", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void multipleTxnTest() {
    REQUIRE(0 == insertTxn("33333", "3aaaa"));
    REQUIRE(0 == insertTxn("33333", "3aaab", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn("33333", "3aaac", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn("11111", "1aaaa"));
    REQUIRE(0 == insertTxn("11111", "1aaab", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn("11111", "1aaac", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn("44444", "4aaaa"));
    REQUIRE(0 == insertTxn("44444", "4aaab", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn("44444", "4aaac", HAM_DUPLICATE));

    REQUIRE(0 == move   ("11111", "1aaaa", HAM_CURSOR_FIRST));
    REQUIRE(0 == move   ("11111", "1aaab", HAM_CURSOR_NEXT));
    REQUIRE(0 == move   ("11111", "1aaac", HAM_CURSOR_NEXT));
    REQUIRE(0 == move   ("33333", "3aaaa", HAM_CURSOR_NEXT));
    REQUIRE(0 == move   ("33333", "3aaab", HAM_CURSOR_NEXT));
    REQUIRE(0 == move   ("33333", "3aaac", HAM_CURSOR_NEXT));
    REQUIRE(0 == move   ("44444", "4aaaa", HAM_CURSOR_NEXT));
    REQUIRE(0 == move   ("44444", "4aaab", HAM_CURSOR_NEXT));
    REQUIRE(0 == move   ("44444", "4aaac", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move   ("44444", "4aaac", HAM_CURSOR_LAST));
    REQUIRE(0 == move   ("44444", "4aaab", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("44444", "4aaaa", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("33333", "3aaac", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("44444", "4aaaa", HAM_CURSOR_NEXT));
    REQUIRE(0 == move   ("33333", "3aaac", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("33333", "3aaab", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("33333", "3aaaa", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("11111", "1aaac", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("11111", "1aaab", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move   ("11111", "1aaaa", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void mixedTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k2", "r2.1"));
    REQUIRE(0 == insertTxn  ("k2", "r2.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.1"));
    REQUIRE(0 == insertTxn  ("k3", "r3.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.3", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.1"));
    REQUIRE(0 == insertBtree("k4", "r4.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.3", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k5", "r5.1"));
    REQUIRE(0 == insertTxn  ("k5", "r5.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.4", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.1"));
    REQUIRE(0 == insertBtree("k6", "r6.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.4", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.6", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.1"));
    REQUIRE(0 == insertBtree("k7", "r7.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k7", "r7.4", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k8", "r8.1"));

    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k2", "r2.1", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k2", "r2.2", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k3", "r3.1", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k3", "r3.2", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k3", "r3.3", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k4", "r4.1", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k4", "r4.2", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k4", "r4.3", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k5", "r5.1", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k5", "r5.2", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k5", "r5.3", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k5", "r5.4", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k6", "r6.1", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k6", "r6.2", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k6", "r6.3", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k6", "r6.4", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k6", "r6.5", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k6", "r6.6", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k7", "r7.1", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k7", "r7.2", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k7", "r7.3", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k7", "r7.4", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k8", "r8.1", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k8", "r8.1", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k7", "r7.4", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k7", "r7.3", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k7", "r7.2", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k7", "r7.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k6", "r6.6", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k6", "r6.5", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k6", "r6.4", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k6", "r6.3", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k6", "r6.2", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k6", "r6.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k5", "r5.4", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k5", "r5.3", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k5", "r5.2", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k5", "r5.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k4", "r4.3", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k4", "r4.2", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k4", "r4.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k3", "r3.3", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k3", "r3.2", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k3", "r3.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k2", "r2.2", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k2", "r2.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void findInDuplicatesTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k2", "r2.1"));
    REQUIRE(0 == insertTxn  ("k2", "r2.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.1"));
    REQUIRE(0 == insertTxn  ("k3", "r3.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.3", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.1"));
    REQUIRE(0 == insertBtree("k4", "r4.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.3", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k5", "r5.1"));
    REQUIRE(0 == insertTxn  ("k5", "r5.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.4", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.1"));
    REQUIRE(0 == insertBtree("k6", "r6.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.4", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.6", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.1"));
    REQUIRE(0 == insertBtree("k7", "r7.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k7", "r7.4", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k8", "r8.1"));

    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.size = 3;

    key.data = (void *)"k1";
    REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r1.1"));

    key.data = (void *)"k2";
    REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r2.1"));

    key.data = (void *)"k3";
    REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r3.1"));

    key.data = (void *)"k4";
    REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r4.1"));

    key.data = (void *)"k5";
    REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r5.1"));

    key.data = (void *)"k6";
    REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r6.1"));

    key.data = (void *)"k7";
    REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r7.1"));

    key.data = (void *)"k8";
    REQUIRE(0 == ham_db_find(m_db, m_txn, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r8.1"));
  }

  void cursorFindInDuplicatesTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k2", "r2.1"));
    REQUIRE(0 == insertTxn  ("k2", "r2.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.1"));
    REQUIRE(0 == insertTxn  ("k3", "r3.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.3", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.1"));
    REQUIRE(0 == insertBtree("k4", "r4.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.3", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k5", "r5.1"));
    REQUIRE(0 == insertTxn  ("k5", "r5.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.4", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.1"));
    REQUIRE(0 == insertBtree("k6", "r6.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.4", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.6", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.1"));
    REQUIRE(0 == insertBtree("k7", "r7.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k7", "r7.4", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k8", "r8.1"));

    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.size = 3;

    key.data = (void *)"k1";
    REQUIRE(0 == ham_cursor_find(m_cursor, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r1.1"));

    key.data = (void *)"k2";
    REQUIRE(0 == ham_cursor_find(m_cursor, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r2.1"));

    key.data = (void *)"k3";
    REQUIRE(0 == ham_cursor_find(m_cursor, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r3.1"));

    key.data = (void *)"k4";
    REQUIRE(0 == ham_cursor_find(m_cursor, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r4.1"));

    key.data = (void *)"k5";
    REQUIRE(0 == ham_cursor_find(m_cursor, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r5.1"));

    key.data = (void *)"k6";
    REQUIRE(0 == ham_cursor_find(m_cursor, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r6.1"));

    key.data = (void *)"k7";
    REQUIRE(0 == ham_cursor_find(m_cursor, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r7.1"));

    key.data = (void *)"k8";
    REQUIRE(0 == ham_cursor_find(m_cursor, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r8.1"));
  }

  void skipDuplicatesTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k2", "r2.1"));
    REQUIRE(0 == insertTxn  ("k2", "r2.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.1"));
    REQUIRE(0 == insertTxn  ("k3", "r3.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.3", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.1"));
    REQUIRE(0 == insertBtree("k4", "r4.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.3", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k5", "r5.1"));
    REQUIRE(0 == insertTxn  ("k5", "r5.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.4", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.1"));
    REQUIRE(0 == insertBtree("k6", "r6.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.4", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.6", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.1"));
    REQUIRE(0 == insertBtree("k7", "r7.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k7", "r7.4", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k8", "r8.1"));

    REQUIRE(0 == move     ("k1", "r1.1",
          HAM_CURSOR_FIRST|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k2", "r2.1",
          HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k3", "r3.1",
          HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k4", "r4.1",
          HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k5", "r5.1",
          HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k6", "r6.1",
          HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k7", "r7.1",
          HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k8", "r8.1",
          HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0,
          HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k8", "r8.1",
          HAM_CURSOR_LAST|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k7", "r7.4",
          HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k6", "r6.6",
          HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k5", "r5.4",
          HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k4", "r4.3",
          HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k3", "r3.3",
          HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k2", "r2.2",
          HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
    REQUIRE(0 == move     ("k1", "r1.1",
          HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0,
          HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES));
  }

  void txnInsertConflictTest() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key = {};
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec = {};

    ham_cursor_t *c;

    /* begin(T1); begin(T2); insert(T1, a); find(T2, a) -> conflict */
    REQUIRE(0 == ham_txn_begin(&txn1, m_env, 0, 0, 0));
    REQUIRE(0 == ham_txn_begin(&txn2, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c, m_db, txn2, 0));
    REQUIRE(0 == ham_db_insert(m_db, txn1, &key, &rec, 0));
    REQUIRE(HAM_TXN_CONFLICT == ham_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ham_cursor_close(c));
    REQUIRE(0 == ham_txn_commit(txn1, 0));
    REQUIRE(0 == ham_txn_commit(txn2, 0));
  }

  void txnEraseConflictTest() {
    ham_txn_t *txn1, *txn2;
    ham_key_t key = {};
    key.data = (void *)"hello";
    key.size = 5;
    ham_record_t rec = {};

    ham_cursor_t *c;

    /* begin(T1); begin(T2); insert(T1, a); find(T2, a) -> conflict */
    REQUIRE(0 == ham_txn_begin(&txn1, m_env, 0, 0, 0));
    REQUIRE(0 == ham_txn_begin(&txn2, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c, m_db, txn2, 0));
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
    REQUIRE(0 == ham_db_insert(m_db, txn1, &key, &rec, HAM_DUPLICATE));
    REQUIRE(HAM_TXN_CONFLICT == ham_db_erase(m_db, 0, &key, 0));
    REQUIRE(0 == ham_cursor_close(c));
    REQUIRE(0 == ham_txn_commit(txn1, 0));
    REQUIRE(0 == ham_txn_commit(txn2, 0));
  }

  void eraseDuplicatesTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k2", "r2.1"));
    REQUIRE(0 == insertTxn  ("k2", "r2.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.1"));
    REQUIRE(0 == insertTxn  ("k3", "r3.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k3", "r3.3", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.1"));
    REQUIRE(0 == insertBtree("k4", "r4.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k4", "r4.3", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k5", "r5.1"));
    REQUIRE(0 == insertTxn  ("k5", "r5.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k5", "r5.4", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.1"));
    REQUIRE(0 == insertBtree("k6", "r6.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k6", "r6.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.4", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k6", "r6.6", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.1"));
    REQUIRE(0 == insertBtree("k7", "r7.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k7", "r7.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k7", "r7.4", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k8", "r8.1"));

    REQUIRE(0 == eraseTxn   ("k1"));
    REQUIRE(0 == eraseTxn   ("k2"));
    REQUIRE(0 == eraseTxn   ("k3"));
    REQUIRE(0 == eraseTxn   ("k4"));
    REQUIRE(0 == eraseTxn   ("k5"));
    REQUIRE(0 == eraseTxn   ("k6"));
    REQUIRE(0 == eraseTxn   ("k7"));
    REQUIRE(0 == eraseTxn   ("k8"));

    REQUIRE(HAM_KEY_NOT_FOUND == find("k1", 0));
    REQUIRE(HAM_KEY_NOT_FOUND == find("k2", 0));
    REQUIRE(HAM_KEY_NOT_FOUND == find("k3", 0));
    REQUIRE(HAM_KEY_NOT_FOUND == find("k4", 0));
    REQUIRE(HAM_KEY_NOT_FOUND == find("k5", 0));
    REQUIRE(HAM_KEY_NOT_FOUND == find("k6", 0));
    REQUIRE(HAM_KEY_NOT_FOUND == find("k7", 0));
    REQUIRE(HAM_KEY_NOT_FOUND == find("k8", 0));
  }

  void cloneDuplicateCursorTest() {
    REQUIRE(0 == insertTxn  ("k1", "r2.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r3.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r3.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r3.3", HAM_DUPLICATE));

    REQUIRE(0 == move("k1", "r2.2", HAM_CURSOR_FIRST));

    ham_cursor_t *c;
    REQUIRE(0 ==
          ham_cursor_clone(m_cursor, &c));

    ham_key_t key = {0};
    ham_record_t rec = {0};
    REQUIRE(0 == ham_cursor_move(c, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r2.2"));
    REQUIRE(0 == strcmp((char *)key.data, "k1"));
    REQUIRE(0 == ham_cursor_close(c));
  }

  void insertCursorCouplesTest() {
    REQUIRE(0 == insertTxn  ("k1", "r2.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r3.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r3.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r3.3", HAM_DUPLICATE));

    ham_key_t key = {0};
    ham_record_t rec = {0};
    REQUIRE(0 == ham_cursor_move(m_cursor, &key, &rec, 0));
    REQUIRE(0 == strcmp((char *)rec.data, "r3.3"));
    REQUIRE(0 == strcmp((char *)key.data, "k1"));
  }

  void insertFirstTest() {
    static const int C = 2;
    /* B 1 3   */
    /* T   5 7 */
    ham_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_create(&c[i], m_db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

    ham_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));

    /* now insert a key at the beginning */
    ham_record_t rec = {0};
    rec.size = 5;
    rec.data = (void *)"r1.2";
    REQUIRE(0 == ham_cursor_insert(c[0], &key, &rec,
          HAM_DUPLICATE | HAM_DUPLICATE_INSERT_FIRST));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.5", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_close(c[i]));
  }

  void insertLastTest() {
    static const int C = 2;
    /* B 1 3   */
    /* T   5 7 */
    ham_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_create(&c[i], m_db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

    ham_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));

    /* now insert a key at the beginning */
    ham_record_t rec = {0};
    rec.size = 5;
    rec.data = (void *)"r1.2";
    REQUIRE(0 == ham_cursor_insert(c[0], &key, &rec,
          HAM_DUPLICATE|HAM_DUPLICATE_INSERT_LAST));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.5", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_LAST));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_close(c[i]));
  }

  void insertAfterTest() {
    static const int C = 4;
    /* B 1 3   */
    /* T   5 7 */
    ham_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_create(&c[i], m_db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

    ham_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));

    REQUIRE(0 ==
          ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_move(c[1], &key, 0, HAM_CURSOR_NEXT));

    REQUIRE(0 ==
          ham_cursor_move(c[2], &key, 0, HAM_CURSOR_LAST));
    REQUIRE(0 ==
          ham_cursor_move(c[2], &key, 0, HAM_CURSOR_PREVIOUS));

    REQUIRE(0 ==
          ham_cursor_move(c[3], &key, 0, HAM_CURSOR_LAST));

    /* now insert keys in-between */
    ham_record_t rec = {0};
    rec.size = 5;
    ham_cursor_t *clone;
    rec.data = (void *)"r1.2";
    REQUIRE(0 == ham_cursor_clone(c[0], &clone));
    REQUIRE(0 == ham_cursor_insert(clone, &key, &rec,
          HAM_DUPLICATE|HAM_DUPLICATE_INSERT_AFTER));
    REQUIRE(0 == ham_cursor_close(clone));

    rec.data = (void *)"r1.4";
    REQUIRE(0 == ham_cursor_clone(c[1], &clone));
    REQUIRE(0 == ham_cursor_insert(clone, &key, &rec,
          HAM_DUPLICATE|HAM_DUPLICATE_INSERT_AFTER));
    REQUIRE(0 == ham_cursor_close(clone));

    rec.data = (void *)"r1.6";
    REQUIRE(0 == ham_cursor_clone(c[2], &clone));
    REQUIRE(0 == ham_cursor_insert(clone, &key, &rec,
          HAM_DUPLICATE|HAM_DUPLICATE_INSERT_AFTER));
    REQUIRE(0 == ham_cursor_close(clone));

    rec.data = (void *)"r1.8";
    REQUIRE(0 == ham_cursor_clone(c[3], &clone));
    REQUIRE(0 == ham_cursor_insert(clone, &key, &rec,
          HAM_DUPLICATE|HAM_DUPLICATE_INSERT_AFTER));
    REQUIRE(0 == ham_cursor_close(clone));

    /* now verify that the original 4 cursors are still coupled to the
     * same duplicate */
    REQUIRE(0 == move     ("k1", "r1.1", 0, c[0]));
    REQUIRE(0 == move     ("k1", "r1.3", 0, c[1]));
    REQUIRE(0 == move     ("k1", "r1.5", 0, c[2]));
    REQUIRE(0 == move     ("k1", "r1.7", 0, c[3]));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.4", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.5", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.6", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.8", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_close(c[i]));
  }

  void insertBeforeTest() {
    const int C = 4;
    /* B 1 3   */
    /* T   5 7 */
    ham_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_create(&c[i], m_db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

    ham_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));

    REQUIRE(0 ==
          ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_move(c[1], &key, 0, HAM_CURSOR_NEXT));

    REQUIRE(0 ==
          ham_cursor_move(c[2], &key, 0, HAM_CURSOR_LAST));
    REQUIRE(0 ==
          ham_cursor_move(c[2], &key, 0, HAM_CURSOR_PREVIOUS));

    REQUIRE(0 ==
          ham_cursor_move(c[3], &key, 0, HAM_CURSOR_LAST));

    /* now insert keys in-between */
    ham_record_t rec = {0};
    rec.size = 5;
    ham_cursor_t *clone;
    rec.data = (void *)"r1.0";
    REQUIRE(0 == ham_cursor_clone(c[0], &clone));
    REQUIRE(0 == ham_cursor_insert(clone, &key, &rec,
          HAM_DUPLICATE|HAM_DUPLICATE_INSERT_BEFORE));
    REQUIRE(0 == ham_cursor_close(clone));

    rec.data = (void *)"r1.2";
    REQUIRE(0 == ham_cursor_clone(c[1], &clone));
    REQUIRE(0 == ham_cursor_insert(clone, &key, &rec,
          HAM_DUPLICATE|HAM_DUPLICATE_INSERT_BEFORE));
    REQUIRE(0 == ham_cursor_close(clone));

    rec.data = (void *)"r1.4";
    REQUIRE(0 == ham_cursor_clone(c[2], &clone));
    REQUIRE(0 == ham_cursor_insert(clone, &key, &rec,
          HAM_DUPLICATE|HAM_DUPLICATE_INSERT_BEFORE));
    REQUIRE(0 == ham_cursor_close(clone));

    rec.data = (void *)"r1.6";
    REQUIRE(0 == ham_cursor_clone(c[3], &clone));
    REQUIRE(0 == ham_cursor_insert(clone, &key, &rec,
          HAM_DUPLICATE|HAM_DUPLICATE_INSERT_BEFORE));
    REQUIRE(0 == ham_cursor_close(clone));

    /* now verify that the original 4 cursors are still coupled to the
     * same duplicate */
    REQUIRE(0 == move     ("k1", "r1.1", 0, c[0]));
    REQUIRE(0 == move     ("k1", "r1.3", 0, c[1]));
    REQUIRE(0 == move     ("k1", "r1.5", 0, c[2]));
    REQUIRE(0 == move     ("k1", "r1.7", 0, c[3]));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.0", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.4", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.5", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.6", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_close(c[i]));
  }

  void extendDupeCacheTest() {
    const int MAX = 512;
    int i = 0;

    for (; i < MAX / 2; i++) {
      char buf[20];
      sprintf(buf, "%d", i);
      REQUIRE(0 == insertBtree("k1", buf, HAM_DUPLICATE));
    }

    for (; i < MAX; i++) {
      char buf[20];
      sprintf(buf, "%d", i);
      REQUIRE(0 == insertTxn  ("k1", buf, HAM_DUPLICATE));
    }

    for (i = 0; i < MAX; i++) {
      char buf[20];
      sprintf(buf, "%d", i);
      REQUIRE(0 == move("k1", buf,
          i == 0 ? HAM_CURSOR_FIRST : HAM_CURSOR_NEXT));
    }
  }

  void overwriteTxnDupeTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    ham_record_t rec = {0};
    rec.size = 5;

    rec.data = (void *)"r2.1";
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_overwrite(m_cursor, &rec, 0));

    rec.data = (void *)"r2.2";
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 ==
          ham_cursor_overwrite(m_cursor, &rec, 0));

    rec.data = (void *)"r2.3";
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 ==
          ham_cursor_overwrite(m_cursor, &rec, 0));

    REQUIRE(0 == move     ("k1", "r2.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r2.2", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r2.3", HAM_CURSOR_NEXT));
  }

  void overwriteBtreeDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));

    ham_record_t rec = {0};
    rec.size = 5;

    rec.data = (void *)"r2.1";
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_overwrite(m_cursor, &rec, 0));

    rec.data = (void *)"r2.2";
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 ==
          ham_cursor_overwrite(m_cursor, &rec, 0));

    rec.data = (void *)"r2.3";
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 ==
          ham_cursor_overwrite(m_cursor, &rec, 0));

    REQUIRE(0 == move     ("k1", "r2.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r2.2", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r2.3", HAM_CURSOR_NEXT));
  }

  void eraseFirstTxnDupeTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseSecondTxnDupeTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseThirdTxnDupeTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesTxnTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_LAST));
  }

  void eraseAllDuplicatesMoveNextTxnTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMovePreviousTxnTest() {
    REQUIRE(0 == insertTxn  ("k0", "r0.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindFirstTxnTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ham_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindLastTxnTest() {
    REQUIRE(0 == insertTxn  ("k0", "r0.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ham_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseFirstBtreeDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));

    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseSecondBtreeDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));

    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseThirdBtreeDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));

    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesBtreeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_LAST));
  }

  void eraseAllDuplicatesMoveNextBtreeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", 0));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k2", "r2.1", 0));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMovePreviousBtreeTest() {
    REQUIRE(0 == insertBtree("k0", "r0.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindFirstBtreeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k2", "r2.1", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ham_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindLastBtreeTest() {
    REQUIRE(0 == insertBtree("k0", "r0.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ham_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseFirstMixedDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseSecondMixedDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseSecondMixedDupeTest2() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseThirdMixedDupeTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseThirdMixedDupeTest2() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMixedTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_LAST));
  }

  void eraseAllDuplicatesMixedTest2() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_LAST));
  }

  void eraseAllDuplicatesMoveNextMixedTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", 0));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", 0));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMoveNextMixedTest2() {
    REQUIRE(0 == insertBtree("k1", "r1.1", 0));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", 0));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMoveNextMixedTest3() {
    REQUIRE(0 == insertBtree("k1", "r1.1", 0));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", 0));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMovePreviousMixedTest() {
    REQUIRE(0 == insertBtree("k0", "r0.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMovePreviousMixedTest2() {
    REQUIRE(0 == insertBtree("k0", "r0.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesMovePreviousMixedTest3() {
    REQUIRE(0 == insertBtree("k0", "r0.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_LAST));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindFirstMixedTest() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ham_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindFirstMixedTest2() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ham_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindFirstMixedTest3() {
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "r2.1", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ham_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k2", "r2.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindLastMixedTest() {
    REQUIRE(0 == insertBtree("k0", "r0.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ham_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindLastMixedTest2() {
    REQUIRE(0 == insertBtree("k0", "r0.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ham_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseAllDuplicatesFindLastMixedTest3() {
    REQUIRE(0 == insertBtree("k0", "r0.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    for (int i = 0; i < 3; i++) {
      ham_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    }

    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move("k0", "r0.1", HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));
  }

  void eraseFirstTest() {
    static const int C = 2;
    /* B 1 3   */
    /* T   5 7 */
    ham_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_create(&c[i], m_db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

    ham_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));

    /* now erase the first key */
    REQUIRE(0 == ham_cursor_erase(c[0], 0));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.5", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.5", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_close(c[i]));
  }

  void eraseLastTest() {
    static const int C = 2;
    /* B 1 3   */
    /* T   5 7 */
    ham_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_create(&c[i], m_db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

    ham_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ham_cursor_move(c[0], &key, 0, HAM_CURSOR_LAST));
    REQUIRE(0 ==
          ham_cursor_move(c[1], &key, 0, HAM_CURSOR_LAST));

    /* now erase the key */
    REQUIRE(0 == ham_cursor_erase(c[0], 0));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.5", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.5", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_close(c[i]));
  }

  void eraseAfterTest() {
    static const int C = 4;
    /* B 1 3   */
    /* T   5 7 */
    ham_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_create(&c[i], m_db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

    ham_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));

    REQUIRE(0 ==
          ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_move(c[1], &key, 0, HAM_CURSOR_NEXT));

    REQUIRE(0 ==
          ham_cursor_move(c[2], &key, 0, HAM_CURSOR_LAST));
    REQUIRE(0 ==
          ham_cursor_move(c[2], &key, 0, HAM_CURSOR_PREVIOUS));

    REQUIRE(0 ==
          ham_cursor_move(c[3], &key, 0, HAM_CURSOR_LAST));

    /* now erase the second key */
    REQUIRE(0 == ham_cursor_erase(c[1], 0));

    /* now verify that the other 3 cursors are still coupled to the
     * same duplicate */
    REQUIRE(0 == move     ("k1", "r1.1", 0, c[0]));
    REQUIRE(HAM_CURSOR_IS_NIL == move("k1", "r1.3", 0, c[1]));
    REQUIRE(0 == move     ("k1", "r1.5", 0, c[2]));
    REQUIRE(0 == move     ("k1", "r1.7", 0, c[3]));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.5", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_close(c[i]));
  }

  void eraseBeforeTest() {
    static const int C = 4;
    /* B 1 3   */
    /* T   5 7 */
    ham_cursor_t *c[C];
    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_create(&c[i], m_db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.5", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.7", HAM_DUPLICATE));

    ham_key_t key = {0};
    key.size = 3;
    key.data = (void *)"k1";

    /* each cursor is positioned on a different duplicate */
    REQUIRE(0 ==
          ham_cursor_move(c[0], &key, 0, HAM_CURSOR_FIRST));

    REQUIRE(0 ==
          ham_cursor_move(c[1], &key, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_move(c[1], &key, 0, HAM_CURSOR_NEXT));

    REQUIRE(0 ==
          ham_cursor_move(c[2], &key, 0, HAM_CURSOR_LAST));
    REQUIRE(0 ==
          ham_cursor_move(c[2], &key, 0, HAM_CURSOR_PREVIOUS));

    REQUIRE(0 ==
          ham_cursor_move(c[3], &key, 0, HAM_CURSOR_LAST));

    /* erase the 3rd key */
    REQUIRE(0 == ham_cursor_erase(c[2], 0));

    /* now verify that the other 3 cursors are still coupled to the
     * same duplicate */
    REQUIRE(0 == move     ("k1", "r1.1", 0, c[0]));
    REQUIRE(0 == move     ("k1", "r1.3", 0, c[1]));
    REQUIRE(HAM_CURSOR_IS_NIL == move("k1", "r1.5", 0, c[2]));
    REQUIRE(0 == move     ("k1", "r1.7", 0, c[3]));

    /* now verify that the keys were inserted in the correct order */
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.7", HAM_CURSOR_LAST));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_PREVIOUS));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_PREVIOUS));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_PREVIOUS));

    for (int i = 0; i < C; i++)
      REQUIRE(0 == ham_cursor_close(c[i]));
  }

  void eraseWithCursorTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    REQUIRE(0 == ham_cursor_erase(m_cursor, 0));

    /* now verify that the last duplicate was erased */
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
  }

  void overwriteWithCursorTest() {
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));

    ham_record_t rec = {0};
    rec.size = 5;
    rec.data = (void *)"r1.4";
    REQUIRE(0 == ham_cursor_overwrite(m_cursor, &rec, 0));

    /* now verify that the last duplicate was overwritten */
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.4", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
  }

  ham_u32_t count(const char *key, ham_status_t st = 0) {
    ham_u32_t c = 0;

    ham_key_t k = {0};
    k.data = (void *)key;
    k.size = strlen(key) + 1;

    REQUIRE(st ==
          ham_cursor_find(m_cursor, &k, 0, 0));
    if (st)
      return (0);
    REQUIRE(0 ==
          ham_cursor_get_duplicate_count(m_cursor, &c, 0));
    return (c);
  }

  void negativeCountTest() {
    REQUIRE(0u == count("k1", HAM_KEY_NOT_FOUND));
  }

  void countTxnTest() {
    REQUIRE(0u == count("k1", HAM_KEY_NOT_FOUND));
    REQUIRE(0 == insertTxn  ("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(1u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(2u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(3u == count("k1"));
  }

  void countBtreeTest() {
    REQUIRE(0u == count("k1", HAM_KEY_NOT_FOUND));
    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(1u == count("k1"));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(2u == count("k1"));
    REQUIRE(0 == insertBtree("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(3u == count("k1"));
  }

  void countMixedTest() {
    REQUIRE(0u == count("k1", HAM_KEY_NOT_FOUND));
    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(1u == count("k1"));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(2u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(3u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.4", HAM_DUPLICATE));
    REQUIRE(4u == count("k1"));
  }

  void countMixedOverwriteTest() {
    REQUIRE(0u == count("k1", HAM_KEY_NOT_FOUND));
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(1u == count("k1"));
    REQUIRE(0 == insertBtree("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(2u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(3u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.4", HAM_DUPLICATE));
    REQUIRE(4u == count("k1"));

    ham_record_t rec = {0};
    rec.size = 5;

    rec.data = (void *)"r2.1";
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
          ham_cursor_overwrite(m_cursor, &rec, 0));

    REQUIRE(4u == count("k1"));

    rec.data = (void *)"r2.2";
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 ==
          ham_cursor_overwrite(m_cursor, &rec, 0));

    REQUIRE(4u == count("k1"));

    rec.data = (void *)"r2.3";
    REQUIRE(0 ==
          ham_cursor_move(m_cursor, 0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 ==
          ham_cursor_overwrite(m_cursor, &rec, 0));

    REQUIRE(4u == count("k1"));
  }

  void countMixedErasedTest() {
    REQUIRE(0u == count("k0", HAM_KEY_NOT_FOUND));
    REQUIRE(0u == count("k1", HAM_KEY_NOT_FOUND));
    REQUIRE(0 == insertBtree("k0", "r0.1", HAM_DUPLICATE));
    REQUIRE(1u == count("k0"));
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(1u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(2u == count("k1"));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(3u == count("k1"));

    for (int i = 0; i < 3; i++) {
      ham_key_t key = {0};
      key.size = 3;
      key.data = (void *)"k1";
      REQUIRE(0 ==
          ham_cursor_find(m_cursor, &key, 0, 0));
      REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
      REQUIRE((unsigned)(2 - i) == count("k1", i == 2 ? HAM_KEY_NOT_FOUND : 0));
    }
  }

  void negativeWithoutDupesTest() {
    teardown();

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 13, 0, 0));
    REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&m_cursor, m_db, m_txn, 0));

    REQUIRE(0 == insertBtree("k1", "r1.1"));
    REQUIRE(1u == count("k1"));
    REQUIRE(0 == insertTxn  ("k2", "r2.1"));
    REQUIRE(1u == count("k1"));

    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));
    ham_u32_t c;
    REQUIRE(HAM_CURSOR_IS_NIL ==
          ham_cursor_get_duplicate_count(m_cursor, &c, 0));
  }

  void nullDupesTest() {
    REQUIRE(0 == insertBtree("k0", 0, HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", 0, HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", 0, HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", 0, HAM_DUPLICATE));
    REQUIRE(1u == count("k0"));
    REQUIRE(3u == count("k1"));

    REQUIRE(0 == move     ("k0", 0, HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", 0, HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
  }

  void tinyDupesTest() {
    REQUIRE(0 == insertBtree("k0", "r0.1", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "r1.1", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "r1.3", HAM_DUPLICATE));
    REQUIRE(1u == count("k0"));
    REQUIRE(3u == count("k1"));

    REQUIRE(0 == move     ("k0", "r0.1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "r1.1", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.2", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "r1.3", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
  }

  void smallDupesTest() {
    REQUIRE(0 == insertBtree("k0", "0000000", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "1111111", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "2222222", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "3333333", HAM_DUPLICATE));
    REQUIRE(1u == count("k0"));
    REQUIRE(3u == count("k1"));

    REQUIRE(0 == move     ("k0", "0000000", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "1111111", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "2222222", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "3333333", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
  }

  void bigDupesTest() {
    REQUIRE(0 == insertBtree("k0", "0000000000", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "1111111111", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "2222222222", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "3333333333", HAM_DUPLICATE));
    REQUIRE(1u == count("k0"));
    REQUIRE(3u == count("k1"));

    REQUIRE(0 == move     ("k0", "0000000000", HAM_CURSOR_FIRST));
    REQUIRE(0 == move     ("k1", "1111111111", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "2222222222", HAM_CURSOR_NEXT));
    REQUIRE(0 == move     ("k1", "3333333333", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
  }

  void conflictFirstTest() {
    REQUIRE(0 == insertTxn  ("k1", "1"));
    REQUIRE(0 == insertTxn  ("k2", "2"));

    ham_txn_t *txn;
    ham_cursor_t *c;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c, m_db, txn, 0));
    REQUIRE(HAM_TXN_CONFLICT ==
          move("k1", "1", HAM_CURSOR_FIRST, c));
    REQUIRE(0 == ham_cursor_close(c));
    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void conflictFirstTest2() {
    REQUIRE(0 == insertTxn  ("k0", "0"));
    REQUIRE(0 == insertBtree("k1", "1"));
    REQUIRE(0 == insertTxn  ("k2", "2"));
    REQUIRE(0 == insertBtree("k3", "3"));

    ham_txn_t *txn;
    ham_cursor_t *c;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c, m_db, txn, 0));
    REQUIRE(HAM_TXN_CONFLICT == move(0, 0, HAM_CURSOR_FIRST, c));
    REQUIRE(0 == ham_cursor_close(c));
    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void conflictLastTest() {
    REQUIRE(0 == insertTxn  ("k0", "0"));
    REQUIRE(0 == insertTxn  ("k1", "1"));

    ham_txn_t *txn;
    ham_cursor_t *c;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c, m_db, txn, 0));
    REQUIRE(HAM_TXN_CONFLICT ==
          move("k1", "1", HAM_CURSOR_LAST, c));
    REQUIRE(0 == ham_cursor_close(c));
    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void conflictLastTest2() {
    REQUIRE(0 == insertBtree("k0", "0"));
    REQUIRE(0 == insertTxn  ("k1", "1"));
    REQUIRE(0 == insertBtree("k2", "0"));
    REQUIRE(0 == insertTxn  ("k3", "1"));

    ham_txn_t *txn;
    ham_cursor_t *c;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c, m_db, txn, 0));
    REQUIRE(HAM_TXN_CONFLICT ==
          move("k3", "1", HAM_CURSOR_LAST, c));
    REQUIRE(0 == ham_cursor_close(c));
    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void conflictNextTest() {
    REQUIRE(0 == insertBtree("k0", "0"));
    REQUIRE(0 == insertBtree("k1", "1"));
    REQUIRE(0 == insertBtree("k1", "2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "2"));
    REQUIRE(0 == insertBtree("k3", "3"));

    ham_txn_t *txn;
    ham_cursor_t *c;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c, m_db, txn, 0));
    REQUIRE(0 == move("k0", "0", HAM_CURSOR_FIRST, c));
    REQUIRE(0 == move("k3", "3", HAM_CURSOR_NEXT, c));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        move(0, 0, HAM_CURSOR_NEXT, c));
    REQUIRE(0 == ham_cursor_close(c));
    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void conflictPreviousTest() {
    REQUIRE(0 == insertBtree("k0", "0"));
    REQUIRE(0 == insertBtree("k1", "1"));
    REQUIRE(0 == insertBtree("k1", "2", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k1", "3", HAM_DUPLICATE));
    REQUIRE(0 == insertTxn  ("k2", "2"));
    REQUIRE(0 == insertBtree("k3", "3"));

    ham_txn_t *txn;
    ham_cursor_t *c;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c, m_db, txn, 0));
    REQUIRE(0 == move("k3", "3", HAM_CURSOR_LAST, c));
    REQUIRE(0 == move("k0", "0", HAM_CURSOR_PREVIOUS, c));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        move(0, 0, HAM_CURSOR_PREVIOUS, c));
    REQUIRE(0 == ham_cursor_close(c));
    REQUIRE(0 == ham_txn_abort(txn, 0));
  }

  void insertDupeConflictsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));

    /* create a second txn, insert a duplicate -> conflict */
    ham_txn_t *txn2;
    REQUIRE(0 == ham_txn_begin(&txn2, m_env, 0, 0, 0));

    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(HAM_TXN_CONFLICT ==
          ham_db_insert(m_db, txn2, &key, &rec, 0));

    REQUIRE(0 == ham_txn_commit(txn2, 0));
  }

  void eraseDupeConflictsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));

    /* create a second txn, insert a duplicate -> conflict */
    ham_txn_t *txn2;
    REQUIRE(0 == ham_txn_begin(&txn2, m_env, 0, 0, 0));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(HAM_TXN_CONFLICT ==
          ham_db_erase(m_db, txn2, &key, 0));

    REQUIRE(0 == ham_txn_commit(txn2, 0));
  }

  void findDupeConflictsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));

    /* create a second txn, insert a duplicate -> conflict */
    ham_txn_t *txn2;
    REQUIRE(0 == ham_txn_begin(&txn2, m_env, 0, 0, 0));

    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(HAM_TXN_CONFLICT ==
          ham_db_find(m_db, txn2, &key, &rec, 0));

    REQUIRE(0 == ham_txn_commit(txn2, 0));
  }

  void cursorInsertDupeConflictsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));

    /* create a second txn, insert a duplicate -> conflict */
    ham_txn_t *txn2;
    ham_cursor_t *c;
    REQUIRE(0 == ham_txn_begin(&txn2, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c, m_db, txn2, 0));

    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(HAM_TXN_CONFLICT ==
          ham_cursor_insert(c, &key, &rec, 0));

    REQUIRE(0 == ham_cursor_close(c));
    REQUIRE(0 == ham_txn_commit(txn2, 0));
  }

  void cursorFindDupeConflictsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));

    /* create a second txn, insert a duplicate -> conflict */
    ham_txn_t *txn2;
    ham_cursor_t *c;
    REQUIRE(0 == ham_txn_begin(&txn2, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c, m_db, txn2, 0));

    ham_key_t key = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(HAM_TXN_CONFLICT ==
          ham_cursor_find(c, &key, 0, 0));

    REQUIRE(0 == ham_cursor_close(c));
    REQUIRE(0 == ham_txn_commit(txn2, 0));
  }

  void flushErasedDupeTest() {
    REQUIRE(0 == insertBtree("k1", "1"));
    REQUIRE(0 == insertBtree("k1", "2", HAM_DUPLICATE));
    REQUIRE(0 == insertBtree("k1", "3", HAM_DUPLICATE));

    /* erase k1/2 */
    REQUIRE(0 == move("k1", "1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move("k1", "2", HAM_CURSOR_NEXT));
    REQUIRE(0 ==
          ham_cursor_erase(m_cursor, 0));

    /* flush the transaction to disk */
    REQUIRE(0 == ham_cursor_close(m_cursor));
    REQUIRE(0 == ham_txn_commit(m_txn, 0));
    REQUIRE(0 == ham_txn_begin(&m_txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&m_cursor, m_db, m_txn, 0));

    /* verify that the duplicate was erased */
    REQUIRE(0 == move("k1", "1", HAM_CURSOR_FIRST));
    REQUIRE(0 == move("k1", "3", HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND == move(0, 0, HAM_CURSOR_NEXT));
  }
};

TEST_CASE("Cursor-dupes/simpleBtreeTest", "")
{
  DupeCursorFixture f;
  f.simpleBtreeTest();
}

TEST_CASE("Cursor-dupes/multipleBtreeTest", "")
{
  DupeCursorFixture f;
  f.multipleBtreeTest();
}

TEST_CASE("Cursor-dupes/simpleTxnInsertLastTest", "")
{
  DupeCursorFixture f;
  f.simpleTxnInsertLastTest();
}

TEST_CASE("Cursor-dupes/simpleTxnInsertFirstTest", "")
{
  DupeCursorFixture f;
  f.simpleTxnInsertFirstTest();
}

TEST_CASE("Cursor-dupes/multipleTxnTest", "")
{
  DupeCursorFixture f;
  f.multipleTxnTest();
}

TEST_CASE("Cursor-dupes/mixedTest", "")
{
  DupeCursorFixture f;
  f.mixedTest();
}

TEST_CASE("Cursor-dupes/findInDuplicatesTest", "")
{
  DupeCursorFixture f;
  f.findInDuplicatesTest();
}

TEST_CASE("Cursor-dupes/cursorFindInDuplicatesTest", "")
{
  DupeCursorFixture f;
  f.cursorFindInDuplicatesTest();
}

TEST_CASE("Cursor-dupes/skipDuplicatesTest", "")
{
  DupeCursorFixture f;
  f.skipDuplicatesTest();
}

TEST_CASE("Cursor-dupes/txnInsertConflictTest", "")
{
  DupeCursorFixture f;
  f.txnInsertConflictTest();
}

TEST_CASE("Cursor-dupes/txnEraseConflictTest", "")
{
  DupeCursorFixture f;
  f.txnEraseConflictTest();
}

TEST_CASE("Cursor-dupes/eraseDuplicatesTest", "")
{
  DupeCursorFixture f;
  f.eraseDuplicatesTest();
}

TEST_CASE("Cursor-dupes/cloneDuplicateCursorTest", "")
{
  DupeCursorFixture f;
  f.cloneDuplicateCursorTest();
}

TEST_CASE("Cursor-dupes/insertCursorCouplesTest", "")
{
  DupeCursorFixture f;
  f.insertCursorCouplesTest();
}

TEST_CASE("Cursor-dupes/insertFirstTest", "")
{
  DupeCursorFixture f;
  f.insertFirstTest();
}

TEST_CASE("Cursor-dupes/insertLastTest", "")
{
  DupeCursorFixture f;
  f.insertLastTest();
}

TEST_CASE("Cursor-dupes/insertAfterTest", "")
{
  DupeCursorFixture f;
  f.insertAfterTest();
}

TEST_CASE("Cursor-dupes/insertBeforeTest", "")
{
  DupeCursorFixture f;
  f.insertBeforeTest();
}

TEST_CASE("Cursor-dupes/extendDupeCacheTest", "")
{
  DupeCursorFixture f;
  f.extendDupeCacheTest();
}

TEST_CASE("Cursor-dupes/overwriteTxnDupeTest", "")
{
  DupeCursorFixture f;
  f.overwriteTxnDupeTest();
}

TEST_CASE("Cursor-dupes/overwriteBtreeDupeTest", "")
{
  DupeCursorFixture f;
  f.overwriteBtreeDupeTest();
}

TEST_CASE("Cursor-dupes/eraseFirstTxnDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseFirstTxnDupeTest();
}

TEST_CASE("Cursor-dupes/eraseSecondTxnDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseSecondTxnDupeTest();
}

TEST_CASE("Cursor-dupes/eraseThirdTxnDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseThirdTxnDupeTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesTxnTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesTxnTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesMoveNextTxnTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMoveNextTxnTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesMovePreviousTxnTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMovePreviousTxnTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesFindFirstTxnTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindFirstTxnTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesFindLastTxnTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindLastTxnTest();
}

TEST_CASE("Cursor-dupes/eraseFirstBtreeDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseFirstBtreeDupeTest();
}

TEST_CASE("Cursor-dupes/eraseSecondBtreeDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseSecondBtreeDupeTest();
}

TEST_CASE("Cursor-dupes/eraseThirdBtreeDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseThirdBtreeDupeTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesBtreeTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesBtreeTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesMoveNextBtreeTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMoveNextBtreeTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesMovePreviousBtreeTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMovePreviousBtreeTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesFindFirstBtreeTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindFirstBtreeTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesFindLastBtreeTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindLastBtreeTest();
}

TEST_CASE("Cursor-dupes/eraseFirstMixedDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseFirstMixedDupeTest();
}

TEST_CASE("Cursor-dupes/eraseSecondMixedDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseSecondMixedDupeTest();
}

TEST_CASE("Cursor-dupes/eraseSecondMixedDupeTest2", "")
{
  DupeCursorFixture f;
  f.eraseSecondMixedDupeTest2();
}

TEST_CASE("Cursor-dupes/eraseThirdMixedDupeTest", "")
{
  DupeCursorFixture f;
  f.eraseThirdMixedDupeTest();
}

TEST_CASE("Cursor-dupes/eraseThirdMixedDupeTest2", "")
{
  DupeCursorFixture f;
  f.eraseThirdMixedDupeTest2();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesMixedTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMixedTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesMixedTest2", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMixedTest2();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesMoveNextMixedTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMoveNextMixedTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesMoveNextMixedTest2", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMoveNextMixedTest2();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesMoveNextMixedTest3", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMoveNextMixedTest3();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesMovePreviousMixedTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMovePreviousMixedTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesMovePreviousMixedTest2", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMovePreviousMixedTest2();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesMovePreviousMixedTest3", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesMovePreviousMixedTest3();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesFindFirstMixedTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindFirstMixedTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesFindFirstMixedTest2", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindFirstMixedTest2();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesFindFirstMixedTest3", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindFirstMixedTest3();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesFindLastMixedTest", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindLastMixedTest();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesFindLastMixedTest2", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindLastMixedTest2();
}

TEST_CASE("Cursor-dupes/eraseAllDuplicatesFindLastMixedTest3", "")
{
  DupeCursorFixture f;
  f.eraseAllDuplicatesFindLastMixedTest3();
}

TEST_CASE("Cursor-dupes/eraseFirstTest", "")
{
  DupeCursorFixture f;
  f.eraseFirstTest();
}

TEST_CASE("Cursor-dupes/eraseLastTest", "")
{
  DupeCursorFixture f;
  f.eraseLastTest();
}

TEST_CASE("Cursor-dupes/eraseAfterTest", "")
{
  DupeCursorFixture f;
  f.eraseAfterTest();
}

TEST_CASE("Cursor-dupes/eraseBeforeTest", "")
{
  DupeCursorFixture f;
  f.eraseBeforeTest();
}

TEST_CASE("Cursor-dupes/eraseWithCursorTest", "")
{
  DupeCursorFixture f;
  f.eraseWithCursorTest();
}

TEST_CASE("Cursor-dupes/overwriteWithCursorTest", "")
{
  DupeCursorFixture f;
  f.overwriteWithCursorTest();
}

TEST_CASE("Cursor-dupes/negativeCountTest", "")
{
  DupeCursorFixture f;
  f.negativeCountTest();
}

TEST_CASE("Cursor-dupes/countTxnTest", "")
{
  DupeCursorFixture f;
  f.countTxnTest();
}

TEST_CASE("Cursor-dupes/countBtreeTest", "")
{
  DupeCursorFixture f;
  f.countBtreeTest();
}

TEST_CASE("Cursor-dupes/countMixedTest", "")
{
  DupeCursorFixture f;
  f.countMixedTest();
}

TEST_CASE("Cursor-dupes/countMixedOverwriteTest", "")
{
  DupeCursorFixture f;
  f.countMixedOverwriteTest();
}

TEST_CASE("Cursor-dupes/countMixedErasedTest", "")
{
  DupeCursorFixture f;
  f.countMixedErasedTest();
}

TEST_CASE("Cursor-dupes/negativeWithoutDupesTest", "")
{
  DupeCursorFixture f;
  f.negativeWithoutDupesTest();
}

TEST_CASE("Cursor-dupes/nullDupesTest", "")
{
  DupeCursorFixture f;
  f.nullDupesTest();
}

TEST_CASE("Cursor-dupes/tinyDupesTest", "")
{
  DupeCursorFixture f;
  f.tinyDupesTest();
}

TEST_CASE("Cursor-dupes/smallDupesTest", "")
{
  DupeCursorFixture f;
  f.smallDupesTest();
}

TEST_CASE("Cursor-dupes/bigDupesTest", "")
{
  DupeCursorFixture f;
  f.bigDupesTest();
}

TEST_CASE("Cursor-dupes/conflictFirstTest", "")
{
  DupeCursorFixture f;
  f.conflictFirstTest();
}

TEST_CASE("Cursor-dupes/conflictFirstTest2", "")
{
  DupeCursorFixture f;
  f.conflictFirstTest2();
}

TEST_CASE("Cursor-dupes/conflictLastTest", "")
{
  DupeCursorFixture f;
  f.conflictLastTest();
}

TEST_CASE("Cursor-dupes/conflictLastTest2", "")
{
  DupeCursorFixture f;
  f.conflictLastTest2();
}

TEST_CASE("Cursor-dupes/conflictNextTest", "")
{
  DupeCursorFixture f;
  f.conflictNextTest();
}

TEST_CASE("Cursor-dupes/conflictPreviousTest", "")
{
  DupeCursorFixture f;
  f.conflictPreviousTest();
}

TEST_CASE("Cursor-dupes/insertDupeConflictsTest", "")
{
  DupeCursorFixture f;
  f.insertDupeConflictsTest();
}

TEST_CASE("Cursor-dupes/eraseDupeConflictsTest", "")
{
  DupeCursorFixture f;
  f.eraseDupeConflictsTest();
}

TEST_CASE("Cursor-dupes/findDupeConflictsTest", "")
{
  DupeCursorFixture f;
  f.findDupeConflictsTest();
}

TEST_CASE("Cursor-dupes/cursorInsertDupeConflictsTest", "")
{
  DupeCursorFixture f;
  f.cursorInsertDupeConflictsTest();
}

TEST_CASE("Cursor-dupes/cursorFindDupeConflictsTest", "")
{
  DupeCursorFixture f;
  f.cursorFindDupeConflictsTest();
}

TEST_CASE("Cursor-dupes/flushErasedDupeTest", "")
{
  DupeCursorFixture f;
  f.flushErasedDupeTest();
}

