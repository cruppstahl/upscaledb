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
#include <cstring>
#include <vector>

#include <ham/hamsterdb.h>

#include "../src/btree_cursor.h"
#include "../src/db.h"
#include "../src/page.h"
#include "../src/error.h"
#include "../src/btree.h"
#include "../src/env.h"
#include "../src/cursor.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace ham;

class BtreeCursorTest : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  BtreeCursorTest(bool inmemory = false, ham_size_t pagesize = 0,
          const char *name = "BtreeCursorTest")
    : hamsterDB_fixture(name), m_db(0), m_inmemory(inmemory),
        m_pagesize(pagesize) {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(BtreeCursorTest, createCloseTest);
    BFC_REGISTER_TEST(BtreeCursorTest, cloneTest);
    BFC_REGISTER_TEST(BtreeCursorTest, moveTest);
    BFC_REGISTER_TEST(BtreeCursorTest, moveSplitTest);
    BFC_REGISTER_TEST(BtreeCursorTest, overwriteTest);
    BFC_REGISTER_TEST(BtreeCursorTest, linkedListTest);
    BFC_REGISTER_TEST(BtreeCursorTest, linkedListReverseCloseTest);
    BFC_REGISTER_TEST(BtreeCursorTest, cursorGetErasedItemTest);
    BFC_REGISTER_TEST(BtreeCursorTest, couplingTest);
  }

protected:
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;
  ham_size_t m_pagesize;

public:
  virtual void setup() {
    __super::setup();

    ham_parameter_t params[] = {
      // set pagesize, otherwise 16-bit limit bugs in freelist
      // will fire on Win32
      { HAM_PARAM_PAGESIZE, (m_pagesize ? m_pagesize : 4096) },
      { 0, 0 }
    };

    os::unlink(BFC_OPATH(".test"));

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_DUPLICATES
              | (m_inmemory ? HAM_IN_MEMORY : 0),
            0664, params));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  virtual void teardown() {
    __super::teardown();

    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void createCloseTest() {
    ham_cursor_t *c;

    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
    BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
  }

  void cloneTest() {
    ham_cursor_t *cursor, *clone;

    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));
    BFC_ASSERT(cursor != 0);
    Cursor *c = new Cursor(*(Cursor *)cursor);
    clone = (ham_cursor_t *)c;
    BFC_ASSERT(clone != 0);
    BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));
    BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
  }

  void overwriteTest() {
    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t rec;
    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));
    int x = 5;
    key.size = sizeof(x);
    key.data = &x;
    rec.size = sizeof(x);
    rec.data = &x;

    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));
    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_overwrite(cursor, &rec, 0));

    BtreeBackend *be = (BtreeBackend *)((Database *)m_db)->get_backend();
    Page *page;
    BFC_ASSERT_EQUAL(0,
        db_fetch_page(&page, (Database *)m_db, be->get_rootpage(), 0));
    BFC_ASSERT(page != 0);
    BFC_ASSERT_EQUAL(0, page->uncouple_all_cursors());

    BFC_ASSERT_EQUAL(0, ham_cursor_overwrite(cursor, &rec, 0));

    BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
  }

  void moveSplitTest() {
    ham_cursor_t *cursor, *cursor2, *cursor3;
    ham_key_t key;
    ham_record_t rec;
    ham_parameter_t p1[] = {
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ham_parameter_t p2[] = {
      { HAM_PARAM_KEYSIZE, 128 },
      { 0, 0 }
    };
    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));

    teardown();
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            (m_inmemory ? HAM_IN_MEMORY : 0),
            0664, &p1[0]));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, &p2[0]));

    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));
    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor2));
    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor3));

    for (int i = 0; i < 64; i++) {
      key.size = sizeof(i);
      key.data = &i;
      rec.size = sizeof(i);
      rec.data = &i;

      BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    BFC_ASSERT_EQUAL(0,
        ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_FIRST));
    BFC_ASSERT_EQUAL(0, *(int *)key.data);
    BFC_ASSERT_EQUAL(0, *(int *)rec.data);
    BFC_ASSERT_EQUAL(0,
        ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_LAST));
    BFC_ASSERT_EQUAL(63, *(int *)key.data);
    BFC_ASSERT_EQUAL(63, *(int *)rec.data);

    for (int i = 0; i < 64; i++) {
      BFC_ASSERT_EQUAL(0,
          ham_cursor_move(cursor2, &key, &rec, HAM_CURSOR_NEXT));
      BFC_ASSERT_EQUAL(i, *(int *)key.data);
      BFC_ASSERT_EQUAL(i, *(int *)rec.data);
    }
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
        ham_cursor_move(cursor2, 0, 0, HAM_CURSOR_NEXT));
    for (int i = 63; i >= 0; i--) {
      BFC_ASSERT_EQUAL(0,
          ham_cursor_move(cursor3, &key, &rec, HAM_CURSOR_PREVIOUS));
      BFC_ASSERT_EQUAL(i, *(int *)key.data);
      BFC_ASSERT_EQUAL(i, *(int *)rec.data);
    }
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
        ham_cursor_move(cursor3, 0, 0, HAM_CURSOR_PREVIOUS));

    BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor2));
    BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor3));
  }

  void moveTest() {
    ham_cursor_t *cursor;

    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

    /* no move, and cursor is nil: returns 0 if key/rec is 0 */
    BFC_ASSERT_EQUAL(0,
          ham_cursor_move(cursor, 0, 0, 0));

    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          ham_cursor_move(cursor, 0, 0, HAM_CURSOR_FIRST));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          ham_cursor_move(cursor, 0, 0, HAM_CURSOR_NEXT));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          ham_cursor_move(cursor, 0, 0, HAM_CURSOR_LAST));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
          ham_cursor_move(cursor, 0, 0, HAM_CURSOR_PREVIOUS));

    BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
  }

  void linkedListTest() {
    ham_cursor_t *cursor[5], *clone;

    BFC_ASSERT_EQUAL((ham_cursor_t *)0, ((Database *)m_db)->get_cursors());

    for (int i = 0; i < 5; i++) {
      BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor[i]));
      BFC_ASSERT_EQUAL(cursor[i], ((Database *)m_db)->get_cursors());
    }

    BFC_ASSERT_EQUAL(0, ham_cursor_clone(cursor[0], &clone));
    BFC_ASSERT(clone != 0);
    BFC_ASSERT_EQUAL(clone, ((Database *)m_db)->get_cursors());

    for (int i = 0; i < 5; i++) {
      BFC_ASSERT_EQUAL(0,
          ham_cursor_close(cursor[i]));
    }
    BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));

    BFC_ASSERT_EQUAL((ham_cursor_t *)0, ((Database *)m_db)->get_cursors());
  }

  void linkedListReverseCloseTest() {
    ham_cursor_t *cursor[5], *clone;

    BFC_ASSERT_EQUAL((ham_cursor_t *)0, ((Database *)m_db)->get_cursors());

    for (int i = 0; i < 5; i++) {
      BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor[i]));
      BFC_ASSERT(cursor[i] != 0);
      BFC_ASSERT_EQUAL(cursor[i], ((Database *)m_db)->get_cursors());
    }

    BFC_ASSERT_EQUAL(0, ham_cursor_clone(cursor[0], &clone));
    BFC_ASSERT(clone != 0);
    BFC_ASSERT_EQUAL((ham_cursor_t *)clone,
        ((Database *)m_db)->get_cursors());

    for (int i = 4; i >= 0; i--) {
      BFC_ASSERT_EQUAL(0,
          ham_cursor_close(cursor[i]));
    }
    BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));

    BFC_ASSERT_EQUAL((ham_cursor_t *)0,
        ((Database *)m_db)->get_cursors());
  }

  void cursorGetErasedItemTest() {
    ham_cursor_t *cursor, *cursor2;
    ham_key_t key;
    ham_record_t rec;
    int value = 0;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    key.data = &value;
    key.size = sizeof(value);

    value = 1;
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, 0, &key, &rec, 0));
    value = 2;
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, 0, &key, &rec, 0));

    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));
    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor2));
    value = 1;
    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_erase(m_db, 0, &key, 0));
    BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL,
        ham_cursor_move(cursor, &key, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_move(cursor, &key, 0, HAM_CURSOR_FIRST));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_move(cursor2, &key, 0, HAM_CURSOR_FIRST));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_erase(cursor, 0));
    BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL,
        ham_cursor_move(cursor2, &key, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor2));
  }

  void couplingTest() {
    ham_cursor_t *c, *clone;
    BtreeCursor *btc;
    ham_key_t key1, key2, key3;
    ham_record_t rec;
    int v1 = 1, v2 = 2, v3 = 3;

    memset(&key1, 0, sizeof(key1));
    memset(&key2, 0, sizeof(key2));
    memset(&key3, 0, sizeof(key3));
    key1.size = sizeof(int);
    key1.data = (void *)&v1;
    key2.size = sizeof(int);
    key2.data = (void *)&v2;
    key3.size = sizeof(int);
    key3.data = (void *)&v3;
    memset(&rec, 0, sizeof(rec));

    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
    btc = ((Cursor *)c)->get_btree_cursor();
    /* after create: cursor is NIL */
    BFC_ASSERT(!btc->is_coupled());
    BFC_ASSERT(!btc->is_uncoupled());

    /* after insert: cursor is NIL */
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, 0, &key2, &rec, 0));
    BFC_ASSERT(!btc->is_coupled());
    BFC_ASSERT(!btc->is_uncoupled());

    /* move to item: cursor is coupled */
    BFC_ASSERT_EQUAL(0, ham_cursor_find(c, &key2, 0, 0));
    BFC_ASSERT(btc->is_coupled());
    BFC_ASSERT(!btc->is_uncoupled());

    /* clone the coupled cursor */
    BFC_ASSERT_EQUAL(0, ham_cursor_clone(c, &clone));
    BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));

    /* insert item BEFORE the first item - cursor is uncoupled */
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, 0, &key1, &rec, 0));
    BFC_ASSERT(!btc->is_coupled());
    BFC_ASSERT(btc->is_uncoupled());

    /* move to item: cursor is coupled */
    BFC_ASSERT_EQUAL(0, ham_cursor_find(c, &key2, 0, 0));
    BFC_ASSERT(btc->is_coupled());
    BFC_ASSERT(!btc->is_uncoupled());

    /* insert duplicate - cursor stays coupled */
    BFC_ASSERT_EQUAL(0,
        ham_db_insert(m_db, 0, &key2, &rec, HAM_DUPLICATE));
    BFC_ASSERT(btc->is_coupled());
    BFC_ASSERT(!btc->is_uncoupled());

    /* insert item AFTER the middle item - cursor stays coupled */
    BFC_ASSERT_EQUAL(0,
        ham_db_insert(m_db, 0, &key3, &rec, 0));
    BFC_ASSERT(btc->is_coupled());
    BFC_ASSERT(!btc->is_uncoupled());

    BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
  }

};

class BtreeCursorTest64Kpage : public BtreeCursorTest {
public:
  BtreeCursorTest64Kpage(bool inmemory = false, ham_size_t pagesize = 64*1024,
      const char *name = "BtreeCursorTest64Kpage")
    : BtreeCursorTest(inmemory, pagesize, name) {
  }
};

class InMemoryBtreeCursorTest : public BtreeCursorTest {
public:
  InMemoryBtreeCursorTest(ham_size_t pagesize = 0,
      const char *name = "InMemoryBtreeCursorTest")
    : BtreeCursorTest(true, pagesize, name) {
  }
};

class InMemoryBtreeCursorTest64Kpage : public InMemoryBtreeCursorTest {
public:
  InMemoryBtreeCursorTest64Kpage(ham_size_t pagesize = 64*1024,
      const char *name = "InMemoryBtreeCursorTest64Kpage")
    : InMemoryBtreeCursorTest(pagesize, name) {
  }
};

BFC_REGISTER_FIXTURE(BtreeCursorTest);
BFC_REGISTER_FIXTURE(InMemoryBtreeCursorTest);

BFC_REGISTER_FIXTURE(BtreeCursorTest64Kpage);
BFC_REGISTER_FIXTURE(InMemoryBtreeCursorTest64Kpage);


