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

#include "4cursor/cursor_local.h"

#include "fixture.hpp"

using namespace upscaledb;

static bool
cursor_is_nil(LocalCursor *c, int what)
{
  return c->is_nil(what);
}

struct BaseCursorFixture : BaseFixture {
  ups_cursor_t *m_cursor;
  ups_txn_t *m_txn;
  ScopedPtr<Context> context;

  BaseCursorFixture()
    : m_cursor(0), m_txn(0) {
  }

  ~BaseCursorFixture() {
    teardown();
  }

  virtual void setup() {
    require_create(UPS_ENABLE_TRANSACTIONS, nullptr,
                      UPS_ENABLE_DUPLICATE_KEYS, nullptr);
    REQUIRE(0 == createCursor(&m_cursor));
    context.reset(new Context(lenv(), 0, 0));
  }

  virtual void teardown() {
    if (context.get())
      context->changeset.clear();

    if (m_cursor) {
      REQUIRE(0 == ups_cursor_close(m_cursor));
      m_cursor = nullptr;
    }

    close();
  }

  virtual ups_status_t createCursor(ups_cursor_t **p) {
    return ups_cursor_create(p, db, 0, 0);
  }

  void getDuplicateRecordSizeTest() {
    const int MAX = 20;
    ups_cursor_t *c;

    char data[16];
    ups_key_t key = {0};
    ups_record_t rec = ups_make_record(data, sizeof(data));

    REQUIRE(0 == ups_cursor_create(&c, db, m_txn, 0));

    for (int i = 0; i < MAX; i++) {
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 == ups_cursor_insert(c, &key, &rec, UPS_DUPLICATE));
    }

    for (int i = 0; i < MAX; i++) {
      uint32_t size = 0;

      REQUIRE(0 == ups_cursor_move(c, &key, &rec,
                i == 0 ? UPS_CURSOR_FIRST : UPS_CURSOR_NEXT));
      REQUIRE(0 == ups_cursor_get_record_size(c, &size));
      REQUIRE(size == rec.size);
    }

    REQUIRE(0 == ups_cursor_close(c));
  }

  void getRecordSizeTest() {
    const int MAX = 20;
    ups_cursor_t *c;
    char data[16];
    ups_key_t key = ups_make_key(data, sizeof(data));
    ups_record_t rec = ups_make_record(data, sizeof(data));

    REQUIRE(0 == ups_cursor_create(&c, db, m_txn, 0));

    for (int i = 0; i < MAX; i++) {
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 == ups_cursor_insert(c, &key, &rec, UPS_DUPLICATE));
    }

    for (int i = 0; i < MAX; i++) {
      uint32_t size = 0;

      REQUIRE(0 == ups_cursor_move(c, &key, &rec,
                      i == 0 ? UPS_CURSOR_FIRST : UPS_CURSOR_NEXT));
      REQUIRE(0 == ups_cursor_get_record_size(c, &size));
      REQUIRE(size == rec.size);
    }

    REQUIRE(0 == ups_cursor_close(c));
  }

  void insertFindTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);

    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    REQUIRE(UPS_DUPLICATE_KEY ==
          ups_cursor_insert(m_cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, UPS_OVERWRITE));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key, &rec, 0));
    REQUIRE(1u ==
          ((LocalCursor *)m_cursor)->duplicate_cache_count(context.get()));
  }

  void insertFindMultipleCursorsTest() {
    ups_cursor_t *c[5];
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);

    for (int i = 0; i < 5; i++)
      REQUIRE(0 == createCursor(&c[i]));

    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ups_cursor_find(c[i], &key, 0, 0));

    REQUIRE(0 == ups_cursor_move(m_cursor, &key, &rec, 0));
    REQUIRE(0 == ::strcmp("12345", (char *)key.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec.data));

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_cursor_move(c[i], &key, &rec, 0));
      REQUIRE(0 == ::strcmp("12345", (char *)key.data));
      REQUIRE(0 == ::strcmp("abcde", (char *)rec.data));
      REQUIRE(0 == ups_cursor_close(c[i]));
    }
  }

  void findInEmptyDatabaseTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);

    // this looks up a key in an empty database
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_find(m_cursor, &key, 0, 0));
  }

  void nilCursorTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);

    // cursor is nil
    REQUIRE(UPS_CURSOR_IS_NIL == ups_cursor_move(m_cursor, &key, &rec, 0));
    REQUIRE(UPS_CURSOR_IS_NIL == ups_cursor_overwrite(m_cursor, &rec, 0));

    ups_cursor_t *clone;
    REQUIRE(0 == ups_cursor_clone(m_cursor, &clone));
    REQUIRE(true == cursor_is_nil((LocalCursor *)m_cursor, 0));
    REQUIRE(true == cursor_is_nil((LocalCursor *)clone, 0));
    REQUIRE(0 == ups_cursor_close(clone));
  }
};

struct TempTxnCursorFixture : public BaseCursorFixture {
  TempTxnCursorFixture() {
    setup();
  }

  void cloneCoupledBtreeCursorTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);

    ups_cursor_t *clone;
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_clone(m_cursor, &clone));
    REQUIRE(cursor_is_nil((LocalCursor *)clone, LocalCursor::kBtree)
              == cursor_is_nil((LocalCursor *)m_cursor, LocalCursor::kBtree));
    REQUIRE(0 == ups_cursor_close(clone));
  }

  void cloneUncoupledBtreeCursorTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);

    LocalCursor *c = (LocalCursor *)m_cursor;
    ups_cursor_t *clone;
    REQUIRE(0 == btree_index()->insert(context.get(), (LocalCursor *)m_cursor,
                            &key, &rec, 0));
    c->btree_cursor.uncouple_from_page(context.get());
    REQUIRE(0 == ups_cursor_clone(m_cursor, &clone));

    ups_key_t *k1 = c->btree_cursor.uncoupled_key();
    ups_key_t *k2 = ((LocalCursor *)clone)->btree_cursor.uncoupled_key();
    REQUIRE(0 == ::strcmp((char *)k1->data, (char *)k2->data));
    REQUIRE(k1->size == k2->size);
    REQUIRE(0 == ups_cursor_close(clone));
  }

  void closeCoupledBtreeCursorTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);

    LocalCursor *c = (LocalCursor *)m_cursor;

    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    c->btree_cursor.uncouple_from_page(context.get());

    /* will close in teardown() */
  }

  void closeUncoupledBtreeCursorTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);

    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* will close in teardown() */
  }
};

TEST_CASE("Cursor/temptxn/insertFindTest", "")
{
  TempTxnCursorFixture f;
  f.insertFindTest();
}

TEST_CASE("Cursor/temptxn/insertFindMultipleCursorsTest", "")
{
  TempTxnCursorFixture f;
  f.insertFindMultipleCursorsTest();
}

TEST_CASE("Cursor/temptxn/findInEmptyDatabaseTest", "")
{
  TempTxnCursorFixture f;
  f.findInEmptyDatabaseTest();
}

TEST_CASE("Cursor/temptxn/nilCursorTest", "")
{
  TempTxnCursorFixture f;
  f.nilCursorTest();
}

TEST_CASE("Cursor/temptxn/cloneCoupledBtreeCursorTest", "")
{
  TempTxnCursorFixture f;
  f.cloneCoupledBtreeCursorTest();
}

TEST_CASE("Cursor/temptxn/cloneUncoupledBtreeCursorTest", "")
{
  TempTxnCursorFixture f;
  f.cloneUncoupledBtreeCursorTest();
}

TEST_CASE("Cursor/temptxn/closeCoupledBtreeCursorTest", "")
{
  TempTxnCursorFixture f;
  f.closeCoupledBtreeCursorTest();
}

TEST_CASE("Cursor/temptxn/closeUncoupledBtreeCursorTest", "")
{
  TempTxnCursorFixture f;
  f.closeUncoupledBtreeCursorTest();
}


struct NoTxnCursorFixture : BaseFixture {
  ups_cursor_t *m_cursor;
  ups_txn_t *m_txn;

  NoTxnCursorFixture() {
    setup();
  }

  ~NoTxnCursorFixture() {
    if (m_cursor) {
      REQUIRE(0 == ups_cursor_close(m_cursor));
      m_cursor = 0;
    }
    close();
  }

  void setup() {
    require_create(0, nullptr, UPS_ENABLE_DUPLICATE_KEYS, nullptr);
    REQUIRE(0 == createCursor(&m_cursor));
  }

  ups_status_t createCursor(ups_cursor_t **p) {
    return ups_cursor_create(p, db, 0, 0);
  }

  void moveFirstInEmptyDatabaseTest() {
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, 0, 0, UPS_CURSOR_FIRST));
  }
};

TEST_CASE("Cursor/notxn/insertFindTest", "")
{
  BaseCursorFixture f;
  f.setup();
  f.insertFindTest();
}

TEST_CASE("Cursor/notxn/insertFindMultipleCursorsTest", "")
{
  BaseCursorFixture f;
  f.setup();
  f.insertFindMultipleCursorsTest();
}

TEST_CASE("Cursor/notxn/findInEmptyDatabaseTest", "")
{
  BaseCursorFixture f;
  f.setup();
  f.findInEmptyDatabaseTest();
}

TEST_CASE("Cursor/notxn/nilCursorTest", "")
{
  BaseCursorFixture f;
  f.setup();
  f.nilCursorTest();
}

TEST_CASE("Cursor/notxn/moveFirstInEmptyDatabaseTest", "")
{
  NoTxnCursorFixture f;
  f.moveFirstInEmptyDatabaseTest();
}

TEST_CASE("Cursor/notxn/getDuplicateRecordSizeTest", "")
{
  BaseCursorFixture f;
  f.setup();
  f.getDuplicateRecordSizeTest();
}

TEST_CASE("Cursor/notxn/getRecordSizeTest", "")
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
    require_create(UPS_IN_MEMORY, nullptr, UPS_ENABLE_DUPLICATE_KEYS, nullptr);
  }
};

TEST_CASE("Cursor/inmem/getDuplicateRecordSizeTest", "")
{
  InMemoryCursorFixture f;
  f.getDuplicateRecordSizeTest();
}

TEST_CASE("Cursor/inmem/getRecordSizeTest", "")
{
  InMemoryCursorFixture f;
  f.getRecordSizeTest();
}


struct LongTxnCursorFixture : public BaseCursorFixture {
  LongTxnCursorFixture() {
    setup();
  }

  virtual void setup() {
    require_create(UPS_ENABLE_TRANSACTIONS, nullptr,
                    UPS_ENABLE_DUPLICATE_KEYS, nullptr);
    REQUIRE(0 == ups_txn_begin(&m_txn, env, 0, 0, 0));
    REQUIRE(0 == createCursor(&m_cursor));
    context.reset(new Context(lenv(), 0, 0));
  }

  virtual ups_status_t createCursor(ups_cursor_t **p) {
    return ups_cursor_create(p, db, m_txn, 0);
  }

  void findInEmptyTxnTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);

    /* insert a key into the btree */
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* this looks up a key in an empty Txn but with the btree */
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == ::strcmp("12345", (char *)key.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec.data));
  }

  void findInBtreeOverwrittenInTxnTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_record_t rec2 = ups_make_record((void *)"22222", 6);

    /* insert a key into the btree */
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* overwrite it in the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec2, UPS_OVERWRITE));

    /* retrieve key and compare record */
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, &rec, 0));
    REQUIRE(0 == ::strcmp("12345", (char *)key.data));
    REQUIRE(0 == ::strcmp("22222", (char *)rec.data));
  }

  void findInTxnOverwrittenInTxnTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_record_t rec2 = ups_make_record((void *)"22222", 6);

    /* insert a key into the txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* overwrite it in the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec2, UPS_OVERWRITE));

    /* retrieve key and compare record */
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, &rec, 0));
    REQUIRE(0 == ::strcmp("12345", (char *)key.data));
    REQUIRE(0 == ::strcmp("22222", (char *)rec.data));
  }

  void eraseInTxnKeyFromBtreeTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);

    /* insert a key into the btree */
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* couple the cursor to this key */
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, 0, 0));

    /* erase it in the Txn */
    REQUIRE(0 == ups_cursor_erase(m_cursor, 0));

    /* key is now nil */
    REQUIRE(true == cursor_is_nil((LocalCursor *)m_cursor, LocalCursor::kBtree));

    /* retrieve key - must fail */
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_find(m_cursor, &key, 0, 0));
  }

  void eraseInTxnKeyFromTxnTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);

    /* insert a key into the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* erase it in the Txn */
    REQUIRE(0 == ups_cursor_erase(m_cursor, 0));

    /* retrieve key - must fail */
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_find(m_cursor, &key, 0, 0));
  }

  void eraseInTxnOverwrittenKeyTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_record_t rec2 = {0};

    /* insert a key into the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* overwrite it in the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec2, UPS_OVERWRITE));

    /* erase it in the Txn */
    REQUIRE(0 == ups_cursor_erase(m_cursor, 0));

    /* retrieve key - must fail */
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_find(m_cursor, &key, 0, 0));
  }

  void eraseInTxnOverwrittenFindKeyTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_record_t rec2 = {0};

    REQUIRE(UPS_CURSOR_IS_NIL == ups_cursor_erase(m_cursor, 0));

    /* insert a key into the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* overwrite it in the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec2, UPS_OVERWRITE));

    /* once more couple the cursor to this key */
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, 0, 0));

    /* erase it in the Txn */
    REQUIRE(0 == ups_cursor_erase(m_cursor, 0));

    /* retrieve key - must fail */
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_find(m_cursor, &key, 0, 0));
  }

  void overwriteInEmptyTxnTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_record_t rec2 = ups_make_record((void *)"aaaaa", 6);

    /* insert a key into the btree */
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* this looks up a key in an empty Txn but with the btree */
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, 0, 0));

    REQUIRE(0 == ups_cursor_overwrite(m_cursor, &rec2, 0));
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, &rec, 0));

    REQUIRE(0 == ::strcmp("12345", (char *)key.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec.data));
  }

  void overwriteInTxnTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_record_t rec2 = ups_make_record((void *)"aaaaa", 6);

    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_overwrite(m_cursor, &rec2, 0));
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, &rec, 0));

    REQUIRE(0 == ::strcmp("12345", (char *)key.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec.data));
  }

  void cloneCoupledTxnCursorTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);

    ups_cursor_t *clone;

    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_clone(m_cursor, &clone));

    LocalCursor *c = (LocalCursor *)m_cursor;
    LocalCursor *cl = (LocalCursor *)clone;

    REQUIRE(2u == ((Txn *)m_txn)->refcounter);
    REQUIRE(c->txn_cursor.get_coupled_op() == cl->txn_cursor.get_coupled_op());
    REQUIRE(0 == ups_cursor_close(clone));
    REQUIRE(1u == ((Txn *)m_txn)->refcounter);

  }

  void closeCoupledTxnCursorTest()
  {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);

    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* will be closed in teardown() */
  }

  void moveFirstInEmptyTxnTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_key_t key2 = {0};
    ups_record_t rec2 = {0};

    /* insert a key into the btree */
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("12345", (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));
  }

  void moveFirstInEmptyTxnExtendedKeyTest() {
    const char *ext = "123456789012345678901234567890";
    ups_key_t key = ups_make_key((void *)ext, 31);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_key_t key2 = {0};
    ups_record_t rec2 = {0};

    /* insert a key into the btree */
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp(ext, (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));
  }

  void moveFirstInTxnTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_key_t key2 = {0};
    ups_record_t rec2 = {0};

    /* insert a key into the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("12345", (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));
  }

  void moveFirstInTxnExtendedKeyTest() {
    const char *ext = "123456789012345678901234567890";
    ups_key_t key = ups_make_key((void *)ext, 31);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_key_t key2 = {0};
    ups_record_t rec2 = {0};

    /* insert a key into the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp(ext, (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));
  }

  void moveFirstIdenticalTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_key_t key2 = {0};
    ups_record_t rec2 = {0};

    /* insert a key into the btree */
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* insert the same key into the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, UPS_OVERWRITE));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("12345", (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));

    /* make sure that the cursor is coupled to the txn-op */
    LocalCursor *c = (LocalCursor *)m_cursor;
    REQUIRE(c->is_txn_active());
  }

  void moveFirstSmallerInTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a large key into the btree */
    key.data = (void *)"22222";
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* insert a smaller key into the Txn */
    key.data = (void *)"11111";
    rec.data = (void *)"xyzab";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("xyzab", (char *)rec2.data));
  }

  void moveFirstSmallerInTxnExtendedKeyTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    const char *ext1 = "111111111111111111111111111111";
    const char *ext2 = "222222222222222222222222222222";
    key.size = 31;
    rec.size = 6;

    /* insert a large key into the btree */
    key.data = (void *)ext2;
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* insert a smaller key into the Txn */
    key.data = (void *)ext1;
    rec.data = (void *)"xyzab";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp(ext1, (char *)key2.data));
    REQUIRE(0 == ::strcmp("xyzab", (char *)rec2.data));
  }

  void moveFirstSmallerInBtreeTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a small key into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* insert a greater key into the Txn */
    key.data = (void *)"22222";
    rec.data = (void *)"xyzab";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));
  }

  void moveFirstSmallerInBtreeExtendedKeyTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    const char *ext1 = "111111111111111111111111111111";
    const char *ext2 = "222222222222222222222222222222";
    key.size = 31;
    rec.size = 6;

    /* insert a small key into the btree */
    key.data = (void *)ext1;
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* insert a greater key into the Txn */
    key.data = (void *)ext2;
    rec.data = (void *)"xyzab";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp(ext1, (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));
  }

  void moveFirstErasedInTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a key into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* erase it */
    key.data = (void *)"11111";
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(m_cursor, 0));

    /* this moves the cursor to the first item, but it was erased
     * and therefore this fails */
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
  }

  void moveFirstErasedInTxnExtendedKeyTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    const char *ext1 = "111111111111111111111111111111";
    key.size = 31;
    rec.size = 6;

    /* insert a key into the btree */
    key.data = (void *)ext1;
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* erase it */
    key.data = (void *)ext1;
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(m_cursor, 0));

    /* this moves the cursor to the first item, but it was erased
     * and therefore this fails */
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
  }

  void moveFirstErasedInsertedInTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a key into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* erase it */
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(m_cursor, 0));

    /* re-insert it */
    rec.data = (void *)"10101";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("10101", (char *)rec2.data));
  }

  void moveFirstSmallerInBtreeErasedInTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a small key into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* insert a greater key into the Txn */
    key.data = (void *)"22222";
    rec.data = (void *)"xyzab";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* erase the smaller item */
    key.data = (void *)"11111";
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(m_cursor, 0));

    /* this moves the cursor to the second item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("xyzab", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  void moveLastInEmptyTxnTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_key_t key2 = {0};
    ups_record_t rec2 = {0};

    /* insert a key into the btree */
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* this moves the cursor to the last item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("12345", (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));
  }

  void moveLastInEmptyTxnExtendedKeyTest() {
    const char *ext = "123456789012345678901234567890";
    ups_key_t key = ups_make_key((void *)ext, 31);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_key_t key2 = {0};
    ups_record_t rec2 = {0};

    /* insert a key into the btree */
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* this moves the cursor to the last item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp(ext, (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));
  }

  void moveLastInTxnTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_key_t key2 = {0};
    ups_record_t rec2 = {0};

    /* insert a key into the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("12345", (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));
  }

  void moveLastInTxnExtendedKeyTest() {
    const char *ext = "123456789012345678901234567890";
    ups_key_t key = ups_make_key((void *)ext, 31);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_key_t key2 = {0};
    ups_record_t rec2 = {0};

    /* insert a key into the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp(ext, (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));
  }

  void moveLastIdenticalTest() {
    ups_key_t key = ups_make_key((void *)"12345", 6);
    ups_record_t rec = ups_make_record((void *)"abcde", 6);
    ups_key_t key2 = {0};
    ups_record_t rec2 = {0};

    /* insert a key into the btree */
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* insert the same key into the Txn */
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, UPS_OVERWRITE));

    /* this moves the cursor to the last item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("12345", (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));

    /* make sure that the cursor is coupled to the txn-op */
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
  }

  void moveLastSmallerInTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a large key into the btree */
    key.data = (void *)"22222";
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* insert a smaller key into the Txn */
    key.data = (void *)"11111";
    rec.data = (void *)"xyzab";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));
  }

  void moveLastSmallerInTxnExtendedKeyTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    const char *ext1 = "111111111111111111111111111111";
    const char *ext2 = "222222222222222222222222222222";
    key.size = 31;
    rec.size = 6;

    /* insert a large key into the btree */
    key.data = (void *)ext2;
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* insert a smaller key into the Txn */
    key.data = (void *)ext1;
    rec.data = (void *)"xyzab";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp(ext2, (char *)key2.data));
    REQUIRE(0 == ::strcmp("abcde", (char *)rec2.data));
  }

  void moveLastSmallerInBtreeTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a small key into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* insert a greater key into the Txn */
    key.data = (void *)"22222";
    rec.data = (void *)"xyzab";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("xyzab", (char *)rec2.data));
  }

  void moveLastSmallerInBtreeExtendedKeyTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    const char *ext1 = "111111111111111111111111111111";
    const char *ext2 = "222222222222222222222222222222";
    key.size = 31;
    rec.size = 6;

    /* insert a small key into the btree */
    key.data = (void *)ext1;
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* insert a greater key into the Txn */
    key.data = (void *)ext2;
    rec.data = (void *)"xyzab";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp(ext2, (char *)key2.data));
    REQUIRE(0 == ::strcmp("xyzab", (char *)rec2.data));
  }

  void moveLastErasedInTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a key into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* erase it */
    key.data = (void *)"11111";
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(m_cursor, 0));

    /* this moves the cursor to the last item, but it was erased
     * and therefore this fails */
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
  }

  void moveLastErasedInTxnExtendedKeyTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    const char *ext1 = "111111111111111111111111111111";
    key.size = 31;
    rec.size = 6;

    /* insert a key into the btree */
    key.data = (void *)ext1;
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* erase it */
    key.data = (void *)ext1;
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(m_cursor, 0));

    /* this moves the cursor to the last item, but it was erased
     * and therefore this fails */
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
  }

  void moveLastErasedInsertedInTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a key into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* erase it */
    key.data = (void *)"11111";
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(m_cursor, 0));

    /* re-insert it */
    rec.data = (void *)"10101";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the last item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("10101", (char *)rec2.data));
  }

  void moveLastSmallerInBtreeErasedInTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a small key into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"abcde";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* insert a greater key into the Txn */
    key.data = (void *)"22222";
    rec.data = (void *)"xyzab";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* erase the smaller item */
    key.data = (void *)"11111";
    REQUIRE(0 == ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(m_cursor, 0));

    /* this moves the cursor to the second item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("xyzab", (char *)rec2.data));
  }

  void moveNextInEmptyTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  void moveNextInEmptyBtreeTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  void moveNextSmallerInTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a "small" key into the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    /* and a "greater" one in the btree */
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  void moveNextSmallerInBtreeTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a "small" key into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* and a "large" one in the txn */
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  void moveNextSmallerInTxnSequenceTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few "small" keys into the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    /* and a few "large" keys in the btree */
    key.data = (void *)"44444";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"55555";
    rec.data = (void *)"eeeee";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"66666";
    rec.data = (void *)"fffff";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("44444", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("55555", (char *)key2.data));
    REQUIRE(0 == ::strcmp("eeeee", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("66666", (char *)key2.data));
    REQUIRE(0 == ::strcmp("fffff", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  void moveNextSmallerInBtreeSequenceTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few "small" keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* and a few "large" keys in the transaction */
    key.data = (void *)"44444";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"55555";
    rec.data = (void *)"eeeee";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"66666";
    rec.data = (void *)"fffff";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("44444", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("55555", (char *)key2.data));
    REQUIRE(0 == ::strcmp("eeeee", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("66666", (char *)key2.data));
    REQUIRE(0 == ::strcmp("fffff", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  void moveNextOverErasedItemTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few "small" keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* erase the one in the middle */
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_db_erase(db, m_txn, &key, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  void moveNextOverIdenticalItemsTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* overwrite the same keys in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  void moveBtreeThenNextOverIdenticalItemsTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"00000";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* skip the first key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(((LocalCursor *)m_cursor)->is_btree_active());
    REQUIRE(0 == ::strcmp("00000", (char *)key2.data));
    REQUIRE(0 == ::strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  void moveTxnThenNextOverIdenticalItemsTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    key.data = (void *)"00000";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, 0));
    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* skip the first key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("00000", (char *)key2.data));
    REQUIRE(0 == ::strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  void moveNextOverIdenticalItemsThenBtreeTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"99999";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* overwrite all keys but the last */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_btree_active());
    REQUIRE(0 == ::strcmp("99999", (char *)key2.data));
    REQUIRE(0 == ::strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  void moveNextOverIdenticalItemsThenTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"99999";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, 0));
    /* skip the first key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_FIRST));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("99999", (char *)key2.data));
    REQUIRE(0 == ::strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_NEXT));
  }

  ups_status_t insertBtree(const char *key, const char *rec,
                  uint32_t flags = 0) {
    ups_key_t k = ups_make_key((void *)key,
                    (uint16_t)(::strlen(key) + 1));
    ups_record_t r = ups_make_record((void *)rec,
                    (uint32_t)(rec ? ::strlen(rec) + 1 : 0));

    ups_status_t st = btree_index()->insert(context.get(), 0, &k, &r, flags);
    context->changeset.clear(); // unlock pages
    return st;
  }

  ups_status_t insertTxn(const char *key, const char *rec,
                  uint32_t flags = 0, ups_cursor_t *cursor = 0) {
    ups_key_t k = ups_make_key((void *)key,
                    (uint16_t)(::strlen(key) + 1));
    ups_record_t r = ups_make_record((void *)rec,
                    (uint32_t)(rec ? ::strlen(rec) + 1 : 0));

    if (cursor)
      return ups_cursor_insert(cursor, &k, &r, flags);
    return ups_db_insert(db, m_txn, &k, &r, flags);
  }

  ups_status_t eraseTxn(const char *key) {
    ups_key_t k = ups_make_key((void *)key, (uint16_t)(::strlen(key) + 1));

    return ups_db_erase(db, m_txn, &k, 0);
  }

#define BTREE 1
#define TXN   2
  ups_status_t compare(const char *key, const char *rec, int where) {
    ups_key_t k = {0};
    ups_record_t r = {0};
    ups_status_t st = ups_cursor_move(m_cursor, &k, &r, UPS_CURSOR_NEXT);
    if (st)
      return st;
    if (::strcmp(key, (char *)k.data))
      return UPS_INTERNAL_ERROR;
    if (::strcmp(rec, (char *)r.data))
      return UPS_INTERNAL_ERROR;
    if (where == BTREE) {
      if (((LocalCursor *)m_cursor)->is_txn_active())
        return UPS_INTERNAL_ERROR;
    }
    else {
      if (((LocalCursor *)m_cursor)->is_btree_active())
        return UPS_INTERNAL_ERROR;
    }
    return 0;
  }

  ups_status_t comparePrev(const char *key, const char *rec, int where) {
    ups_key_t k = {0};
    ups_record_t r = {0};
    ups_status_t st = ups_cursor_move(m_cursor, &k, &r, UPS_CURSOR_PREVIOUS);
    if (st)
      return st;
    if (::strcmp(key, (char *)k.data))
      return UPS_INTERNAL_ERROR;
    if (strcmp(rec, (char *)r.data))
      return UPS_INTERNAL_ERROR;
    if (where == BTREE) {
      if (((LocalCursor *)m_cursor)->is_txn_active())
        return UPS_INTERNAL_ERROR;
    }
    else {
      if (((LocalCursor *)m_cursor)->is_btree_active())
        return UPS_INTERNAL_ERROR;
    }
    return 0;
  }

  void moveNextOverSequencesOfIdenticalItemsTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11113", "aaaaa", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11114", "aaaab"));
    REQUIRE(0 == insertTxn  ("11115", "aaaac"));
    REQUIRE(0 == insertBtree("11116", "aaaaa"));
    REQUIRE(0 == insertBtree("11117", "aaaab"));
    REQUIRE(0 == insertBtree("11118", "aaaac"));
    REQUIRE(0 == insertTxn  ("11116", "bbbba", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11117", "bbbbb", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11118", "bbbbc", UPS_OVERWRITE));

    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaab", BTREE));
    REQUIRE(0 == compare  ("11113", "aaaaa", TXN));
    REQUIRE(0 == compare  ("11114", "aaaab", TXN));
    REQUIRE(0 == compare  ("11115", "aaaac", TXN));
    REQUIRE(0 == compare  ("11116", "bbbba", TXN));
    REQUIRE(0 == compare  ("11117", "bbbbb", TXN));
    REQUIRE(0 == compare  ("11118", "bbbbc", TXN));
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, 0));
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
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, 0));
  }

  void moveNextWhileInsertingTxnTest() {
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
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, 0));
  }

  void moveNextWhileInsertingMixedTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11112", "aaaaa", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11117", "aaaab"));
    REQUIRE(0 == insertTxn  ("11118", "aaaac"));
    REQUIRE(0 == insertBtree("11119", "aaaac"));

    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaaa", TXN));
    REQUIRE(0 == insertTxn  ("11113", "xxxxx", UPS_OVERWRITE));
    REQUIRE(0 == compare  ("11113", "xxxxx", TXN));
    REQUIRE(0 == compare  ("11117", "aaaab", TXN));
    REQUIRE(0 == compare  ("11118", "aaaac", TXN));
    REQUIRE(0 == compare  ("11119", "aaaac", BTREE));
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, 0));
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
    REQUIRE(true == cursor_is_nil((LocalCursor *)m_cursor, 0));
    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11113", "aaaac", BTREE));
    REQUIRE(0 == eraseTxn   ("11114"));
    REQUIRE(0 == compare  ("11115", "aaaae", TXN));
    REQUIRE(0 == compare  ("11116", "aaaaf", TXN));
    REQUIRE(0 == eraseTxn   ("11116"));
    REQUIRE(true == cursor_is_nil((LocalCursor *)m_cursor, 0));
  }

  void movePreviousInEmptyTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
  }

  void movePreviousInEmptyBtreeTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
  }

  void movePreviousSmallerInTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a "small" key into the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    /* and a "large" one in the btree */
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
  }

  void movePreviousSmallerInBtreeTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a "small" key into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* and a "large" one in the txn */
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
  }

  void movePreviousSmallerInTxnSequenceTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few "small" keys into the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    /* and a few "large" keys in the btree */
    key.data = (void *)"44444";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"55555";
    rec.data = (void *)"eeeee";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"66666";
    rec.data = (void *)"fffff";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("66666", (char *)key2.data));
    REQUIRE(0 == ::strcmp("fffff", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("55555", (char *)key2.data));
    REQUIRE(0 == ::strcmp("eeeee", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("44444", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
  }

  void movePreviousSmallerInBtreeSequenceTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few "small" keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* and a few "large" keys in the transaction */
    key.data = (void *)"44444";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"55555";
    rec.data = (void *)"eeeee";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));
    key.data = (void *)"66666";
    rec.data = (void *)"fffff";
    REQUIRE(0 == ups_cursor_insert(m_cursor, &key, &rec, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("66666", (char *)key2.data));
    REQUIRE(0 == ::strcmp("fffff", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("55555", (char *)key2.data));
    REQUIRE(0 == ::strcmp("eeeee", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("44444", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
  }

  void movePreviousOverErasedItemTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few "small" keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* erase the one in the middle */
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_db_erase(db, m_txn, &key, 0));

    /* this moves the cursor to the first item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("aaaaa", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
  }

  void movePreviousOverIdenticalItemsTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* overwrite the same keys in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));

    /* this moves the cursor to the last item */
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 == ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
  }

  void moveBtreeThenPreviousOverIdenticalItemsTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"00000";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* skip the first key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(((LocalCursor *)m_cursor)->is_btree_active());
    REQUIRE(0 == ::strcmp("00000", (char *)key2.data));
    REQUIRE(0 == ::strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
  }

  void moveTxnThenPreviousOverIdenticalItemsTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    key.data = (void *)"00000";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, 0));
    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* skip the first key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("00000", (char *)key2.data));
    REQUIRE(0 == ::strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
  }

  void movePreviousOverIdenticalItemsThenBtreeTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"99999";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    /* skip the last key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 ==
          ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 ==
          ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 ==
          ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(((LocalCursor *)m_cursor)->is_btree_active());
    REQUIRE(0 == ::strcmp("99999", (char *)key2.data));
    REQUIRE(0 == ::strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
  }

  void movePreviousOverIdenticalItemsThenTxnTest() {
    ups_key_t key = {0}, key2 = {0};
    ups_record_t rec = {0}, rec2 = {0};
    key.size = 6;
    rec.size = 6;

    /* insert a few keys into the btree */
    key.data = (void *)"11111";
    rec.data = (void *)"aaaaa";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"22222";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"33333";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == btree_index()->insert(context.get(), 0, &key, &rec, 0));
    context->changeset.clear(); // unlock pages
    key.data = (void *)"99999";
    rec.data = (void *)"xxxxx";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, 0));
    /* skip the first key, and overwrite all others in the transaction */
    key.data = (void *)"11111";
    rec.data = (void *)"bbbbb";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"22222";
    rec.data = (void *)"ccccc";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"33333";
    rec.data = (void *)"ddddd";
    REQUIRE(0 == ups_db_insert(db, m_txn, &key, &rec, UPS_OVERWRITE));

    /* this moves the cursor to the last item */
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_LAST));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("99999", (char *)key2.data));
    REQUIRE(0 == ::strcmp("xxxxx", (char *)rec2.data));
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("33333", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ddddd", (char *)rec2.data));
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("22222", (char *)key2.data));
    REQUIRE(0 == ::strcmp("ccccc", (char *)rec2.data));
    REQUIRE(0 ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
    REQUIRE(((LocalCursor *)m_cursor)->is_txn_active());
    REQUIRE(0 == ::strcmp("11111", (char *)key2.data));
    REQUIRE(0 == ::strcmp("bbbbb", (char *)rec2.data));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(m_cursor, &key2, &rec2, UPS_CURSOR_PREVIOUS));
  }

  void movePreviousOverSequencesOfIdenticalItemsTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11113", "aaaaa", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11114", "aaaab"));
    REQUIRE(0 == insertTxn  ("11115", "aaaac"));
    REQUIRE(0 == insertBtree("11116", "aaaaa"));
    REQUIRE(0 == insertBtree("11117", "aaaab"));
    REQUIRE(0 == insertBtree("11118", "aaaac"));
    REQUIRE(0 == insertTxn  ("11116", "bbbba", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11117", "bbbbb", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11118", "bbbbc", UPS_OVERWRITE));

    REQUIRE(0 == comparePrev("11118", "bbbbc", TXN));
    REQUIRE(0 == comparePrev("11117", "bbbbb", TXN));
    REQUIRE(0 == comparePrev("11116", "bbbba", TXN));
    REQUIRE(0 == comparePrev("11115", "aaaac", TXN));
    REQUIRE(0 == comparePrev("11114", "aaaab", TXN));
    REQUIRE(0 == comparePrev("11113", "aaaaa", TXN));
    REQUIRE(0 == comparePrev("11112", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(UPS_KEY_NOT_FOUND == comparePrev(0, 0, 0));
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
    REQUIRE(UPS_KEY_NOT_FOUND == comparePrev(0, 0, 0));
  }

  void movePreviousWhileInsertingTxnTest() {
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
    REQUIRE(UPS_KEY_NOT_FOUND == comparePrev(0, 0, 0));
  }

  void movePreviousWhileInsertingMixedTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11112", "aaaaa", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11117", "aaaab"));
    REQUIRE(0 == insertTxn  ("11118", "aaaac"));
    REQUIRE(0 == insertBtree("11119", "aaaac"));

    REQUIRE(0 == comparePrev("11119", "aaaac", BTREE));
    REQUIRE(0 == comparePrev("11118", "aaaac", TXN));
    REQUIRE(0 == comparePrev("11117", "aaaab", TXN));
    REQUIRE(0 == insertTxn  ("11113", "xxxxx", UPS_OVERWRITE));
    REQUIRE(0 == comparePrev("11113", "xxxxx", TXN));
    REQUIRE(0 == comparePrev("11112", "aaaaa", TXN));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(UPS_KEY_NOT_FOUND == comparePrev(0, 0, 0));
  }

  void switchDirectionsInBtreeTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("11112", "aaaab"));
    REQUIRE(0 == insertBtree("11113", "aaaac"));
    REQUIRE(0 == insertBtree("11114", "aaaad"));
    REQUIRE(0 == insertBtree("11115", "aaaae"));
    REQUIRE(0 == insertBtree("11116", "aaaaf"));
    REQUIRE(0 == insertBtree("11116", "aaaag", UPS_OVERWRITE));
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

  void switchDirectionsInTxnTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("11112", "aaaab"));
    REQUIRE(0 == insertTxn  ("11113", "aaaac"));
    REQUIRE(0 == insertTxn  ("11114", "aaaad"));
    REQUIRE(0 == insertTxn  ("11115", "aaaae"));
    REQUIRE(0 == insertTxn  ("11116", "aaaaf"));
    REQUIRE(0 == insertTxn  ("11116", "aaaag", UPS_OVERWRITE));
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
    REQUIRE(0 == insertTxn  ("11116", "aaaag", UPS_OVERWRITE));
    REQUIRE(0 == insertBtree("11117", "aaaah"));
    REQUIRE(0 == insertTxn  ("11118", "aaaai"));
    REQUIRE(0 == insertBtree("11119", "aaaaj"));
    REQUIRE(0 == insertTxn  ("11119", "aaaak", UPS_OVERWRITE));

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
    REQUIRE(0 == insertTxn  ("11116", "aaaag", UPS_OVERWRITE));
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
    REQUIRE(0 == insertTxn  ("11113", "aaaae", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11114", "aaaaf", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11115", "aaaag", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11116", "aaaah"));
    REQUIRE(0 == insertTxn  ("11117", "aaaai"));
    REQUIRE(0 == insertBtree("11118", "aaaaj"));
    REQUIRE(0 == insertBtree("11119", "aaaak"));
    REQUIRE(0 == insertBtree("11120", "aaaal"));
    REQUIRE(0 == insertBtree("11121", "aaaam"));
    REQUIRE(0 == insertTxn  ("11120", "aaaan", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11121", "aaaao", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("11122", "aaaap"));

    REQUIRE(0 == compare  ("11111", "aaaaa", BTREE));
    REQUIRE(0 == compare  ("11112", "aaaab", BTREE));
    REQUIRE(0 == compare  ("11113", "aaaae", TXN));
    REQUIRE(0 == compare  ("11114", "aaaaf", TXN));
    REQUIRE(0 == comparePrev("11113", "aaaae", TXN));
    REQUIRE(0 == comparePrev("11112", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(UPS_KEY_NOT_FOUND == comparePrev(0, 0, BTREE));
    ((LocalCursor *)m_cursor)->set_to_nil(0);
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
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, BTREE));
    ((LocalCursor *)m_cursor)->set_to_nil(0);
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
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, BTREE));
  }

  void findTxnThenMoveNextTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertTxn  ("33333", "aaaac"));
    REQUIRE(0 == insertBtree("44444", "aaaad"));
    REQUIRE(0 == insertBtree("55555", "aaaae"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare  ("44444", "aaaad", BTREE));
    REQUIRE(0 == compare  ("55555", "aaaae", BTREE));
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, BTREE));
  }

  void findTxnThenMoveNext2Test() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("44444", "aaaad"));
    REQUIRE(0 == insertBtree("55555", "aaaae"));
    REQUIRE(0 == insertBtree("66666", "aaaaf"));
    REQUIRE(0 == insertTxn  ("77777", "aaaag"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"44444";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare  ("55555", "aaaae", BTREE));
    REQUIRE(0 == compare  ("66666", "aaaaf", BTREE));
    REQUIRE(0 == compare  ("77777", "aaaag", TXN));
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, BTREE));
  }

  void findTxnThenMovePreviousTest() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertTxn  ("33333", "aaaac"));
    REQUIRE(0 == insertBtree("44444", "aaaad"));
    REQUIRE(0 == insertBtree("55555", "aaaae"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == comparePrev("22222", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(UPS_KEY_NOT_FOUND == comparePrev(0, 0, BTREE));
  }

  void findTxnThenMoveNext3Test() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("33333", "aaaad", UPS_OVERWRITE));
    REQUIRE(0 == insertTxn  ("44444", "aaaae"));
    REQUIRE(0 == insertTxn  ("55555", "aaaaf"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare("44444", "aaaae", TXN));
    REQUIRE(0 == compare("55555", "aaaaf", TXN));
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, TXN));
  }

  void findTxnThenMoveNext4Test() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("33333", "aaaad", UPS_OVERWRITE));
    REQUIRE(0 == insertBtree("44444", "aaaae"));
    REQUIRE(0 == insertBtree("55555", "aaaaf"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare("44444", "aaaae", BTREE));
    REQUIRE(0 == compare("55555", "aaaaf", BTREE));
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, TXN));
  }

  void findTxnThenMovePrevious2Test() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("44444", "aaaad"));
    REQUIRE(0 == insertBtree("55555", "aaaae"));
    REQUIRE(0 == insertBtree("66666", "aaaaf"));
    REQUIRE(0 == insertTxn  ("77777", "aaaag"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"44444";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == comparePrev("33333", "aaaac", BTREE));
    REQUIRE(0 == comparePrev("22222", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", TXN));
    REQUIRE(UPS_KEY_NOT_FOUND == comparePrev(0, 0, BTREE));
  }

  void findTxnThenMovePrevious3Test() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("33333", "aaaad", UPS_OVERWRITE));
    REQUIRE(0 == insertBtree("44444", "aaaae"));
    REQUIRE(0 == insertBtree("55555", "aaaaf"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == comparePrev("22222", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(UPS_KEY_NOT_FOUND == comparePrev(0, 0, TXN));
  }

  void findTxnThenMovePrevious4Test() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("33333", "aaaad", UPS_OVERWRITE));
    REQUIRE(0 == insertBtree("44444", "aaaae"));
    REQUIRE(0 == insertBtree("55555", "aaaaf"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == comparePrev("22222", "aaaab", BTREE));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(UPS_KEY_NOT_FOUND == comparePrev(0, 0, TXN));
  }

  void findBtreeThenMoveNextTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("44444", "aaaad"));
    REQUIRE(0 == insertTxn  ("55555", "aaaae"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare  ("44444", "aaaad", TXN));
    REQUIRE(0 == compare  ("55555", "aaaae", TXN));
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, TXN));
  }

  void findBtreeThenMovePreviousTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("44444", "aaaad"));
    REQUIRE(0 == insertTxn  ("55555", "aaaae"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == comparePrev("22222", "aaaab", TXN));
    REQUIRE(0 == comparePrev("11111", "aaaaa", TXN));
    REQUIRE(UPS_KEY_NOT_FOUND == comparePrev(0, 0, TXN));
  }

  void findBtreeThenMovePrevious2Test() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("22222", "aaaab"));
    REQUIRE(0 == insertTxn  ("33333", "aaaac"));
    REQUIRE(0 == insertBtree("44444", "aaaad"));
    REQUIRE(0 == insertTxn  ("55555", "aaaae"));
    REQUIRE(0 == insertTxn  ("66666", "aaaaf"));
    REQUIRE(0 == insertBtree("77777", "aaaag"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"44444";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == comparePrev("33333", "aaaac", TXN));
    REQUIRE(0 == comparePrev("22222", "aaaab", TXN));
    REQUIRE(0 == comparePrev("11111", "aaaaa", BTREE));
    REQUIRE(UPS_KEY_NOT_FOUND == comparePrev(0, 0, BTREE));
  }

  void findBtreeThenMoveNext2Test() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("22222", "aaaab"));
    REQUIRE(0 == insertTxn  ("33333", "aaaac"));
    REQUIRE(0 == insertBtree("44444", "aaaad"));
    REQUIRE(0 == insertTxn  ("55555", "aaaae"));
    REQUIRE(0 == insertTxn  ("66666", "aaaaf"));
    REQUIRE(0 == insertBtree("77777", "aaaag"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"44444";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare  ("55555", "aaaae", TXN));
    REQUIRE(0 == compare  ("66666", "aaaaf", TXN));
    REQUIRE(0 == compare  ("77777", "aaaag", BTREE));
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, BTREE));
  }

  void findBtreeThenMoveNext3Test() {
    REQUIRE(0 == insertBtree("11111", "aaaaa"));
    REQUIRE(0 == insertBtree("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("33333", "aaaad", UPS_OVERWRITE));
    REQUIRE(0 == insertBtree("44444", "aaaae"));
    REQUIRE(0 == insertBtree("55555", "aaaaf"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 == compare("44444", "aaaae", BTREE));
    REQUIRE(0 == compare("55555", "aaaaf", BTREE));
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, TXN));
  }

  void insertThenMoveNextTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    REQUIRE(0 == insertTxn  ("22222", "aaaab"));
    REQUIRE(0 == insertBtree("33333", "aaaac"));
    REQUIRE(0 == insertTxn  ("44444", "aaaad"));
    REQUIRE(0 == insertTxn  ("55555", "aaaae"));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"33333";
    ups_record_t rec = {0};
    rec.size = 6;
    rec.data = (void *)"33333";
    REQUIRE(0 ==
          ups_cursor_insert(m_cursor, &key, &rec, UPS_OVERWRITE));
    REQUIRE(0 == compare  ("44444", "aaaad", TXN));
    REQUIRE(0 == compare  ("55555", "aaaae", TXN));
    REQUIRE(UPS_KEY_NOT_FOUND == compare(0, 0, TXN));
  }

  void abortWhileCursorActiveTest() {
    REQUIRE(UPS_CURSOR_STILL_OPEN == ups_txn_abort(m_txn, 0));
  }

  void commitWhileCursorActiveTest()
  {
    REQUIRE(UPS_CURSOR_STILL_OPEN == ups_txn_commit(m_txn, 0));
  }

  void eraseKeyWithTwoCursorsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    ups_cursor_t *cursor2;
    REQUIRE(0 ==
          ups_cursor_clone(m_cursor, &cursor2));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ups_cursor_find(cursor2, &key, 0, 0));

    REQUIRE(0 ==
          ups_cursor_erase(m_cursor, 0));
    REQUIRE(true == cursor_is_nil((LocalCursor *)m_cursor, 0));
    REQUIRE(true == cursor_is_nil((LocalCursor *)cursor2, 0));

    REQUIRE(0 == ups_cursor_close(cursor2));
  }

  void eraseKeyWithTwoCursorsOverwriteTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    ups_cursor_t *cursor2;
    REQUIRE(0 ==
          ups_cursor_clone(m_cursor, &cursor2));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    ups_record_t rec = {0};
    rec.size = 6;
    rec.data = (void *)"11111";
    REQUIRE(0 ==
          ups_cursor_insert(cursor2, &key, &rec, UPS_OVERWRITE));

    REQUIRE(0 ==
          ups_cursor_erase(m_cursor, 0));
    REQUIRE(true == cursor_is_nil((LocalCursor *)m_cursor, 0));
    REQUIRE(true == cursor_is_nil((LocalCursor *)cursor2, 0));

    REQUIRE(0 == ups_cursor_close(cursor2));
  }

  void eraseWithThreeCursorsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    ups_cursor_t *cursor2, *cursor3;
    REQUIRE(0 ==
          ups_cursor_create(&cursor2, db, m_txn, 0));
    REQUIRE(0 ==
          ups_cursor_create(&cursor3, db, m_txn, 0));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"11111";
    ups_record_t rec = {0};
    rec.size = 6;
    rec.data = (void *)"33333";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ups_cursor_insert(cursor2, &key, &rec, UPS_OVERWRITE));
    REQUIRE(0 ==
          ups_cursor_insert(cursor3, &key, &rec, UPS_OVERWRITE));

    REQUIRE(0 ==
          ups_db_erase(db, m_txn, &key, 0));
    REQUIRE(true == cursor_is_nil((LocalCursor *)m_cursor, 0));
    REQUIRE(true == cursor_is_nil((LocalCursor *)cursor2, 0));
    REQUIRE(true == cursor_is_nil((LocalCursor *)cursor3, 0));

    REQUIRE(0 == ups_cursor_close(cursor2));
    REQUIRE(0 == ups_cursor_close(cursor3));
  }

  void eraseKeyWithoutCursorsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));
    ups_cursor_t *cursor2;
    REQUIRE(0 ==
          ups_cursor_clone(m_cursor, &cursor2));

    ups_key_t key = {0};
    key.size = 6;
    key.data = (void *)"11111";
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));
    REQUIRE(0 ==
          ups_cursor_find(cursor2, &key, 0, 0));

    REQUIRE(UPS_TXN_CONFLICT ==
          ups_db_erase(db, 0, &key, 0));
    REQUIRE(0 ==
          ups_db_erase(db, m_txn, &key, 0));
    REQUIRE(true == cursor_is_nil((LocalCursor *)m_cursor, 0));
    REQUIRE(true == cursor_is_nil((LocalCursor *)cursor2, 0));

    REQUIRE(0 == ups_cursor_close(cursor2));
  }

  void eraseKeyAndFlushTxnsTest() {
    REQUIRE(0 == insertTxn  ("11111", "aaaaa"));

    /* create a second txn, insert and commit, but do not flush the
     * first one */
    ups_txn_t *txn2;
    REQUIRE(0 == ups_txn_begin(&txn2, env, 0, 0, 0));

    ups_cursor_t *cursor2;
    REQUIRE(0 ==
          ups_cursor_create(&cursor2, db, txn2, 0));

    ups_key_t key = {0};
    ups_record_t rec = {0};
    key.size = 6;
    key.data = (void *)"11112";
    REQUIRE(0 ==
          ups_cursor_insert(cursor2, &key, &rec, 0));
    REQUIRE(0 ==
          ups_cursor_close(cursor2));

    /* commit the 2nd txn - it will not be flushed because an older
     * txn also was not flushed */
    REQUIRE(0 == ups_txn_commit(txn2, 0));

    /* the other cursor is part of the first transaction; position on
     * the new key */
    REQUIRE(0 ==
          ups_cursor_find(m_cursor, &key, 0, 0));

    /* now erase the key */
    REQUIRE(0 ==
          ups_db_erase(db, m_txn, &key, 0));

    /* cursor must be nil */
    REQUIRE(true == cursor_is_nil((LocalCursor *)m_cursor, 0));
  }

  ups_status_t move(const char *key, const char *rec, uint32_t flags,
        ups_cursor_t *cursor = 0) {
    ups_key_t k = {0};
    ups_record_t r = {0};
    ups_status_t st;

    if (!cursor)
      cursor = m_cursor;

    st = ups_cursor_move(cursor, &k, &r, flags);
    if (st)
      return (st);
    if (strcmp(key, (char *)k.data))
      return (UPS_INTERNAL_ERROR);
    if (rec)
      if (strcmp(rec, (char *)r.data))
        return (UPS_INTERNAL_ERROR);

    // now verify again, but with flags=0
    if (flags == 0)
      return (0);
    st = ups_cursor_move(cursor, &k, &r, 0);
    if (st)
      return (st);
    if (strcmp(key, (char *)k.data))
      return (UPS_INTERNAL_ERROR);
    if (rec)
      if (strcmp(rec, (char *)r.data))
        return (UPS_INTERNAL_ERROR);
    return (0);
  }
};

TEST_CASE("Cursor/longtxn/getDuplicateRecordSizeTest", "")
{
  LongTxnCursorFixture f;
  f.getDuplicateRecordSizeTest();
}

TEST_CASE("Cursor/longtxn/getRecordSizeTest", "")
{
  LongTxnCursorFixture f;
  f.getRecordSizeTest();
}

TEST_CASE("Cursor/longtxn/insertFindTest", "")
{
  LongTxnCursorFixture f;
  f.insertFindTest();
}

TEST_CASE("Cursor/longtxn/insertFindMultipleCursorsTest", "")
{
  LongTxnCursorFixture f;
  f.insertFindMultipleCursorsTest();
}

TEST_CASE("Cursor/longtxn/findInEmptyDatabaseTest", "")
{
  LongTxnCursorFixture f;
  f.findInEmptyDatabaseTest();
}

TEST_CASE("Cursor/longtxn/findInEmptyTxnTest", "")
{
  LongTxnCursorFixture f;
  f.findInEmptyTxnTest();
}

TEST_CASE("Cursor/longtxn/findInBtreeOverwrittenInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.findInBtreeOverwrittenInTxnTest();
}

TEST_CASE("Cursor/longtxn/findInTxnOverwrittenInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.findInTxnOverwrittenInTxnTest();
}

TEST_CASE("Cursor/longtxn/eraseInTxnKeyFromBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.eraseInTxnKeyFromBtreeTest();
}

TEST_CASE("Cursor/longtxn/eraseInTxnKeyFromTxnTest", "")
{
  LongTxnCursorFixture f;
  f.eraseInTxnKeyFromTxnTest();
}

TEST_CASE("Cursor/longtxn/eraseInTxnOverwrittenKeyTest", "")
{
  LongTxnCursorFixture f;
  f.eraseInTxnOverwrittenKeyTest();
}

TEST_CASE("Cursor/longtxn/eraseInTxnOverwrittenFindKeyTest", "")
{
  LongTxnCursorFixture f;
  f.eraseInTxnOverwrittenFindKeyTest();
}

TEST_CASE("Cursor/longtxn/overwriteInEmptyTxnTest", "")
{
  LongTxnCursorFixture f;
  f.overwriteInEmptyTxnTest();
}

TEST_CASE("Cursor/longtxn/overwriteInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.overwriteInTxnTest();
}

TEST_CASE("Cursor/longtxn/cloneCoupledTxnCursorTest", "")
{
  LongTxnCursorFixture f;
  f.cloneCoupledTxnCursorTest();
}

TEST_CASE("Cursor/longtxn/closeCoupledTxnCursorTest", "")
{
  LongTxnCursorFixture f;
  f.closeCoupledTxnCursorTest();
}

TEST_CASE("Cursor/longtxn/moveFirstInEmptyTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstInEmptyTxnTest();
}

TEST_CASE("Cursor/longtxn/moveFirstInEmptyTxnExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstInEmptyTxnExtendedKeyTest();
}

TEST_CASE("Cursor/longtxn/moveFirstInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstInTxnTest();
}

TEST_CASE("Cursor/longtxn/moveFirstInTxnExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstInTxnExtendedKeyTest();
}

TEST_CASE("Cursor/longtxn/moveFirstIdenticalTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstIdenticalTest();
}

TEST_CASE("Cursor/longtxn/moveFirstSmallerInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstSmallerInTxnTest();
}

TEST_CASE("Cursor/longtxn/moveFirstSmallerInTxnExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstSmallerInTxnExtendedKeyTest();
}

TEST_CASE("Cursor/longtxn/moveFirstSmallerInBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstSmallerInBtreeTest();
}

TEST_CASE("Cursor/longtxn/moveFirstSmallerInBtreeExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstSmallerInBtreeExtendedKeyTest();
}

TEST_CASE("Cursor/longtxn/moveFirstErasedInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstErasedInTxnTest();
}

TEST_CASE("Cursor/longtxn/moveFirstErasedInTxnExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstErasedInTxnExtendedKeyTest();
}

TEST_CASE("Cursor/longtxn/moveFirstErasedInsertedInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstErasedInsertedInTxnTest();
}

TEST_CASE("Cursor/longtxn/moveFirstSmallerInBtreeErasedInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveFirstSmallerInBtreeErasedInTxnTest();
}

TEST_CASE("Cursor/longtxn/moveLastInEmptyTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastInEmptyTxnTest();
}

TEST_CASE("Cursor/longtxn/moveLastInEmptyTxnExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastInEmptyTxnExtendedKeyTest();
}

TEST_CASE("Cursor/longtxn/moveLastInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastInTxnTest();
}

TEST_CASE("Cursor/longtxn/moveLastInTxnExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastInTxnExtendedKeyTest();
}

TEST_CASE("Cursor/longtxn/moveLastIdenticalTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastIdenticalTest();
}

TEST_CASE("Cursor/longtxn/moveLastSmallerInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastSmallerInTxnTest();
}

TEST_CASE("Cursor/longtxn/moveLastSmallerInTxnExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastSmallerInTxnExtendedKeyTest();
}

TEST_CASE("Cursor/longtxn/moveLastSmallerInBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastSmallerInBtreeTest();
}

TEST_CASE("Cursor/longtxn/moveLastSmallerInBtreeExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastSmallerInBtreeExtendedKeyTest();
}

TEST_CASE("Cursor/longtxn/moveLastErasedInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastErasedInTxnTest();
}

TEST_CASE("Cursor/longtxn/moveLastErasedInTxnExtendedKeyTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastErasedInTxnExtendedKeyTest();
}

TEST_CASE("Cursor/longtxn/moveLastErasedInsertedInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastErasedInsertedInTxnTest();
}

TEST_CASE("Cursor/longtxn/moveLastSmallerInBtreeErasedInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveLastSmallerInBtreeErasedInTxnTest();
}

TEST_CASE("Cursor/longtxn/nilCursorTest", "")
{
  LongTxnCursorFixture f;
  f.nilCursorTest();
}

TEST_CASE("Cursor/longtxn/moveNextInEmptyTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextInEmptyTxnTest();
}

TEST_CASE("Cursor/longtxn/moveNextInEmptyBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextInEmptyBtreeTest();
}

TEST_CASE("Cursor/longtxn/moveNextSmallerInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextSmallerInTxnTest();
}

TEST_CASE("Cursor/longtxn/moveNextSmallerInBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextSmallerInBtreeTest();
}

TEST_CASE("Cursor/longtxn/moveNextSmallerInTxnSequenceTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextSmallerInTxnSequenceTest();
}

TEST_CASE("Cursor/longtxn/moveNextSmallerInBtreeSequenceTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextSmallerInBtreeSequenceTest();
}

TEST_CASE("Cursor/longtxn/moveNextOverErasedItemTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextOverErasedItemTest();
}

TEST_CASE("Cursor/longtxn/moveNextOverIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextOverIdenticalItemsTest();
}

TEST_CASE("Cursor/longtxn/moveBtreeThenNextOverIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.moveBtreeThenNextOverIdenticalItemsTest();
}

TEST_CASE("Cursor/longtxn/moveTxnThenNextOverIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.moveTxnThenNextOverIdenticalItemsTest();
}

TEST_CASE("Cursor/longtxn/moveNextOverIdenticalItemsThenBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextOverIdenticalItemsThenBtreeTest();
}

TEST_CASE("Cursor/longtxn/moveNextOverIdenticalItemsThenTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextOverIdenticalItemsThenTxnTest();
}

TEST_CASE("Cursor/longtxn/moveNextOverSequencesOfIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextOverSequencesOfIdenticalItemsTest();
}

TEST_CASE("Cursor/longtxn/moveNextWhileInsertingBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextWhileInsertingBtreeTest();
}

TEST_CASE("Cursor/longtxn/moveNextWhileInsertingTxnTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextWhileInsertingTxnTest();
}

TEST_CASE("Cursor/longtxn/moveNextWhileInsertingMixedTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextWhileInsertingMixedTest();
}

TEST_CASE("Cursor/longtxn/moveNextWhileErasingTest", "")
{
  LongTxnCursorFixture f;
  f.moveNextWhileErasingTest();
}

TEST_CASE("Cursor/longtxn/movePreviousInEmptyTxnTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousInEmptyTxnTest();
}

TEST_CASE("Cursor/longtxn/movePreviousInEmptyBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousInEmptyBtreeTest();
}

TEST_CASE("Cursor/longtxn/movePreviousSmallerInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousSmallerInTxnTest();
}

TEST_CASE("Cursor/longtxn/movePreviousSmallerInBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousSmallerInBtreeTest();
}

TEST_CASE("Cursor/longtxn/movePreviousSmallerInTxnSequenceTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousSmallerInTxnSequenceTest();
}

TEST_CASE("Cursor/longtxn/movePreviousSmallerInBtreeSequenceTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousSmallerInBtreeSequenceTest();
}

TEST_CASE("Cursor/longtxn/movePreviousOverErasedItemTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousOverErasedItemTest();
}

TEST_CASE("Cursor/longtxn/movePreviousOverIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousOverIdenticalItemsTest();
}

TEST_CASE("Cursor/longtxn/moveBtreeThenPreviousOverIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.moveBtreeThenPreviousOverIdenticalItemsTest();
}

TEST_CASE("Cursor/longtxn/moveTxnThenPreviousOverIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.moveTxnThenPreviousOverIdenticalItemsTest();
}

TEST_CASE("Cursor/longtxn/movePreviousOverIdenticalItemsThenBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousOverIdenticalItemsThenBtreeTest();
}

TEST_CASE("Cursor/longtxn/movePreviousOverIdenticalItemsThenTxnTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousOverIdenticalItemsThenTxnTest();
}

TEST_CASE("Cursor/longtxn/movePreviousOverSequencesOfIdenticalItemsTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousOverSequencesOfIdenticalItemsTest();
}

TEST_CASE("Cursor/longtxn/movePreviousWhileInsertingBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousWhileInsertingBtreeTest();
}

TEST_CASE("Cursor/longtxn/movePreviousWhileInsertingTxnTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousWhileInsertingTxnTest();
}

TEST_CASE("Cursor/longtxn/movePreviousWhileInsertingMixedTest", "")
{
  LongTxnCursorFixture f;
  f.movePreviousWhileInsertingMixedTest();
}

TEST_CASE("Cursor/longtxn/switchDirectionsInBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.switchDirectionsInBtreeTest();
}

TEST_CASE("Cursor/longtxn/switchDirectionsInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.switchDirectionsInTxnTest();
}

TEST_CASE("Cursor/longtxn/switchDirectionsMixedStartInBtreeTest", "")
{
  LongTxnCursorFixture f;
  f.switchDirectionsMixedStartInBtreeTest();
}

TEST_CASE("Cursor/longtxn/switchDirectionsMixedStartInTxnTest", "")
{
  LongTxnCursorFixture f;
  f.switchDirectionsMixedStartInTxnTest();
}

TEST_CASE("Cursor/longtxn/switchDirectionsMixedSequenceTest", "")
{
  LongTxnCursorFixture f;
  f.switchDirectionsMixedSequenceTest();
}

TEST_CASE("Cursor/longtxn/findTxnThenMoveNextTest", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMoveNextTest();
}

TEST_CASE("Cursor/longtxn/findTxnThenMoveNext2Test", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMoveNext2Test();
}

TEST_CASE("Cursor/longtxn/findTxnThenMoveNext3Test", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMoveNext3Test();
}

TEST_CASE("Cursor/longtxn/findTxnThenMoveNext4Test", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMoveNext4Test();
}

TEST_CASE("Cursor/longtxn/findTxnThenMovePreviousTest", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMovePreviousTest();
}

TEST_CASE("Cursor/longtxn/findTxnThenMovePrevious2Test", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMovePrevious2Test();
}

TEST_CASE("Cursor/longtxn/findTxnThenMovePrevious3Test", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMovePrevious3Test();
}

TEST_CASE("Cursor/longtxn/findTxnThenMovePrevious4Test", "")
{
  LongTxnCursorFixture f;
  f.findTxnThenMovePrevious4Test();
}

TEST_CASE("Cursor/longtxn/findBtreeThenMoveNextTest", "")
{
  LongTxnCursorFixture f;
  f.findBtreeThenMoveNextTest();
}

TEST_CASE("Cursor/longtxn/findBtreeThenMoveNext2Test", "")
{
  LongTxnCursorFixture f;
  f.findBtreeThenMoveNext2Test();
}

TEST_CASE("Cursor/longtxn/findBtreeThenMoveNext3Test", "")
{
  LongTxnCursorFixture f;
  f.findBtreeThenMoveNext3Test();
}

TEST_CASE("Cursor/longtxn/findBtreeThenMovePreviousTest", "")
{
  LongTxnCursorFixture f;
  f.findBtreeThenMovePreviousTest();
}

TEST_CASE("Cursor/longtxn/findBtreeThenMovePrevious2Test", "")
{
  LongTxnCursorFixture f;
  f.findBtreeThenMovePrevious2Test();
}

TEST_CASE("Cursor/longtxn/insertThenMoveNextTest", "")
{
  LongTxnCursorFixture f;
  f.insertThenMoveNextTest();
}

TEST_CASE("Cursor/longtxn/abortWhileCursorActiveTest", "")
{
  LongTxnCursorFixture f;
  f.abortWhileCursorActiveTest();
}

TEST_CASE("Cursor/longtxn/commitWhileCursorActiveTest", "")
{
  LongTxnCursorFixture f;
  f.commitWhileCursorActiveTest();
}

TEST_CASE("Cursor/longtxn/eraseKeyWithTwoCursorsTest", "")
{
  LongTxnCursorFixture f;
  f.eraseKeyWithTwoCursorsTest();
}

// TODO why was this removed? FC_REGISTER_TEST(LongTxnCursorTest,
      //eraseKeyWithTwoCursorsOverwriteTest);

TEST_CASE("Cursor/longtxn/eraseWithThreeCursorsTest", "")
{
  LongTxnCursorFixture f;
  f.eraseWithThreeCursorsTest();
}

TEST_CASE("Cursor/longtxn/eraseKeyWithoutCursorsTest", "")
{
  LongTxnCursorFixture f;
  f.eraseKeyWithoutCursorsTest();
}

TEST_CASE("Cursor/longtxn/eraseKeyAndFlushTxnsTest", "")
{
  LongTxnCursorFixture f;
  f.eraseKeyAndFlushTxnsTest();
}

