/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
#include "os.hpp"

#include "1base/error.h"
#include "2page/page.h"
#include "3btree/btree_index.h"
#include "3page_manager/page_manager.h"
#include "3btree/btree_cursor.h"
#include "4db/db.h"
#include "4env/env_local.h"
#include "4cursor/cursor.h"

namespace hamsterdb {

struct BtreeCursorFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;
  uint32_t m_page_size;

  BtreeCursorFixture(bool inmemory = false, uint32_t page_size = 0)
    : m_db(0), m_inmemory(inmemory), m_page_size(page_size) {
    ham_parameter_t params[] = {
      // set page_size, otherwise 16-bit limit bugs in freelist
      // will fire on Win32
      { HAM_PARAM_PAGESIZE, (m_page_size ? m_page_size : 4096) },
      { 0, 0 }
    };

    os::unlink(Utils::opath(".test"));

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
              m_inmemory ? HAM_IN_MEMORY : 0, 0664, params));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, HAM_ENABLE_DUPLICATE_KEYS, 0));
  }

  ~BtreeCursorFixture() {
    teardown();
  }

  void teardown() {
    if (m_env)
      REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void createCloseTest() {
    ham_cursor_t *c;

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_close(c));
  }

  void cloneTest() {
    ham_cursor_t *cursor, *clone;

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    REQUIRE(cursor != 0);
    Cursor *c = new Cursor(*(Cursor *)cursor);
    clone = (ham_cursor_t *)c;
    REQUIRE(clone != 0);
    REQUIRE(0 == ham_cursor_close(clone));
    REQUIRE(0 == ham_cursor_close(cursor));
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

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ham_cursor_overwrite(cursor, &rec, 0));

    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    Page *page;
    PageManager *pm = ((LocalEnvironment *)m_env)->get_page_manager();
    REQUIRE((page = pm->fetch_page((LocalDatabase *)m_db,
                            be->get_root_address())));
    REQUIRE(page != 0);
    BtreeCursor::uncouple_all_cursors(page);

    REQUIRE(0 == ham_cursor_overwrite(cursor, &rec, 0));

    REQUIRE(0 == ham_cursor_close(cursor));
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
      { HAM_PARAM_KEYSIZE, 70 },
      { 0, 0 }
    };
    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));

    teardown();
    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
            (m_inmemory ? HAM_IN_MEMORY : 0),
            0664, &p1[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, &p2[0]));

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&cursor2, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&cursor3, m_db, 0, 0));

    char buffer[70] = {0};

    for (int i = 0; i < 64; i++) {
      *(int *)&buffer[0] = i;
      key.size = sizeof(buffer);
      key.data = &buffer[0];
      rec.size = sizeof(buffer);
      rec.data = &buffer[0];

      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    REQUIRE(0 == ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_FIRST));
    REQUIRE(0 == *(int *)key.data);
    REQUIRE(0 == *(int *)rec.data);
    REQUIRE(0 == ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(63 == *(int *)key.data);
    REQUIRE(63 == *(int *)rec.data);

    for (int i = 0; i < 64; i++) {
      REQUIRE(0 == ham_cursor_move(cursor2, &key, &rec, HAM_CURSOR_NEXT));
      REQUIRE(i == *(int *)key.data);
      REQUIRE(i == *(int *)rec.data);
    }
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(cursor2, 0, 0, HAM_CURSOR_NEXT));
    for (int i = 63; i >= 0; i--) {
      REQUIRE(0 == ham_cursor_move(cursor3, &key, &rec, HAM_CURSOR_PREVIOUS));
      REQUIRE(i == *(int *)key.data);
      REQUIRE(i == *(int *)rec.data);
    }
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(cursor3, 0, 0, HAM_CURSOR_PREVIOUS));

    REQUIRE(0 == ham_cursor_close(cursor));
    REQUIRE(0 == ham_cursor_close(cursor2));
    REQUIRE(0 == ham_cursor_close(cursor3));
  }

  void moveTest() {
    ham_cursor_t *cursor;

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    /* no move, and cursor is nil: returns 0 if key/rec is 0 */
    REQUIRE(0 ==
          ham_cursor_move(cursor, 0, 0, 0));

    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(cursor, 0, 0, HAM_CURSOR_FIRST));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(cursor, 0, 0, HAM_CURSOR_NEXT));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(cursor, 0, 0, HAM_CURSOR_LAST));
    REQUIRE(HAM_KEY_NOT_FOUND ==
          ham_cursor_move(cursor, 0, 0, HAM_CURSOR_PREVIOUS));

    REQUIRE(0 == ham_cursor_close(cursor));
  }

  void linkedListTest() {
    ham_cursor_t *cursor[5], *clone;

    REQUIRE((Cursor *)0 == ((LocalDatabase *)m_db)->get_cursor_list());

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ham_cursor_create(&cursor[i], m_db, 0, 0));
      REQUIRE((Cursor *)cursor[i]
                      == ((LocalDatabase *)m_db)->get_cursor_list());
    }

    REQUIRE(0 == ham_cursor_clone(cursor[0], &clone));
    REQUIRE(clone != 0);
    REQUIRE((Cursor *)clone == ((LocalDatabase *)m_db)->get_cursor_list());

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 ==
          ham_cursor_close(cursor[i]));
    }
    REQUIRE(0 == ham_cursor_close(clone));

    REQUIRE((Cursor *)0 == ((LocalDatabase *)m_db)->get_cursor_list());
  }

  void linkedListReverseCloseTest() {
    ham_cursor_t *cursor[5], *clone;

    REQUIRE((Cursor *)0 == ((LocalDatabase *)m_db)->get_cursor_list());

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ham_cursor_create(&cursor[i], m_db, 0, 0));
      REQUIRE(cursor[i] != 0);
      REQUIRE((Cursor *)cursor[i] == ((LocalDatabase *)m_db)->get_cursor_list());
    }

    REQUIRE(0 == ham_cursor_clone(cursor[0], &clone));
    REQUIRE(clone != 0);
    REQUIRE((Cursor *)clone == ((LocalDatabase *)m_db)->get_cursor_list());

    for (int i = 4; i >= 0; i--) {
      REQUIRE(0 == ham_cursor_close(cursor[i]));
    }
    REQUIRE(0 == ham_cursor_close(clone));

    REQUIRE((Cursor *)0 == ((LocalDatabase *)m_db)->get_cursor_list());
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
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    value = 2;
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&cursor2, m_db, 0, 0));
    value = 1;
    REQUIRE(0 == ham_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ham_db_erase(m_db, 0, &key, 0));
    REQUIRE(HAM_CURSOR_IS_NIL ==
        ham_cursor_move(cursor, &key, 0, 0));
    REQUIRE(0 ==
        ham_cursor_move(cursor, &key, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
        ham_cursor_move(cursor2, &key, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
        ham_cursor_erase(cursor, 0));
    REQUIRE(HAM_CURSOR_IS_NIL ==
        ham_cursor_move(cursor2, &key, 0, 0));

    REQUIRE(0 == ham_cursor_close(cursor));
    REQUIRE(0 == ham_cursor_close(cursor2));
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

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));
    btc = ((Cursor *)c)->get_btree_cursor();
    /* after create: cursor is NIL */
    REQUIRE(btc->get_state() != BtreeCursor::kStateCoupled);
    REQUIRE(btc->get_state() != BtreeCursor::kStateUncoupled);

    /* after insert: cursor is NIL */
    REQUIRE(0 == ham_db_insert(m_db, 0, &key2, &rec, 0));
    REQUIRE(btc->get_state() != BtreeCursor::kStateCoupled);
    REQUIRE(btc->get_state() != BtreeCursor::kStateUncoupled);

    /* move to item: cursor is coupled */
    REQUIRE(0 == ham_cursor_find(c, &key2, 0, 0));
    REQUIRE(btc->get_state() == BtreeCursor::kStateCoupled);
    REQUIRE(btc->get_state() != BtreeCursor::kStateUncoupled);

    /* clone the coupled cursor */
    REQUIRE(0 == ham_cursor_clone(c, &clone));
    REQUIRE(0 == ham_cursor_close(clone));

    /* insert item BEFORE the first item - cursor is uncoupled */
    REQUIRE(0 == ham_db_insert(m_db, 0, &key1, &rec, 0));
    REQUIRE(btc->get_state() != BtreeCursor::kStateCoupled);
    REQUIRE(btc->get_state() == BtreeCursor::kStateUncoupled);

    /* move to item: cursor is coupled */
    REQUIRE(0 == ham_cursor_find(c, &key2, 0, 0));
    REQUIRE(btc->get_state() == BtreeCursor::kStateCoupled);
    REQUIRE(btc->get_state() != BtreeCursor::kStateUncoupled);

    /* insert duplicate - cursor stays coupled */
    REQUIRE(0 == ham_db_insert(m_db, 0, &key2, &rec, HAM_DUPLICATE));
    REQUIRE(btc->get_state() == BtreeCursor::kStateCoupled);
    REQUIRE(btc->get_state() != BtreeCursor::kStateUncoupled);

    /* insert item AFTER the middle item - cursor stays coupled */
    REQUIRE(0 == ham_db_insert(m_db, 0, &key3, &rec, 0));
    REQUIRE(btc->get_state() == BtreeCursor::kStateCoupled);
    REQUIRE(btc->get_state() != BtreeCursor::kStateUncoupled);

    REQUIRE(0 == ham_cursor_close(c));
  }

};

TEST_CASE("BtreeCursor/createCloseTest", "")
{
  BtreeCursorFixture f;
  f.createCloseTest();
}

TEST_CASE("BtreeCursor/cloneTest", "")
{
  BtreeCursorFixture f;
  f.cloneTest();
}

TEST_CASE("BtreeCursor/moveTest", "")
{
  BtreeCursorFixture f;
  f.moveTest();
}

TEST_CASE("BtreeCursor/moveSplitTest", "")
{
  BtreeCursorFixture f;
  f.moveSplitTest();
}

TEST_CASE("BtreeCursor/overwriteTest", "")
{
  BtreeCursorFixture f;
  f.overwriteTest();
}

TEST_CASE("BtreeCursor/linkedListTest", "")
{
  BtreeCursorFixture f;
  f.linkedListTest();
}

TEST_CASE("BtreeCursor/linkedListReverseCloseTest", "")
{
  BtreeCursorFixture f;
  f.linkedListReverseCloseTest();
}

TEST_CASE("BtreeCursor/cursorGetErasedItemTest", "")
{
  BtreeCursorFixture f;
  f.cursorGetErasedItemTest();
}

TEST_CASE("BtreeCursor/couplingTest", "")
{
  BtreeCursorFixture f;
  f.couplingTest();
}


TEST_CASE("BtreeCursor-64k/createCloseTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.createCloseTest();
}

TEST_CASE("BtreeCursor-64k/cloneTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.cloneTest();
}

TEST_CASE("BtreeCursor-64k/moveTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.moveTest();
}

TEST_CASE("BtreeCursor-64k/moveSplitTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.moveSplitTest();
}

TEST_CASE("BtreeCursor-64k/overwriteTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.overwriteTest();
}

TEST_CASE("BtreeCursor-64k/linkedListTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.linkedListTest();
}

TEST_CASE("BtreeCursor-64k/linkedListReverseCloseTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.linkedListReverseCloseTest();
}

TEST_CASE("BtreeCursor-64k/cursorGetErasedItemTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.cursorGetErasedItemTest();
}

TEST_CASE("BtreeCursor-64k/couplingTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.couplingTest();
}


TEST_CASE("BtreeCursor-inmem/createCloseTest", "")
{
  BtreeCursorFixture f(true);
  f.createCloseTest();
}

TEST_CASE("BtreeCursor-inmem/cloneTest", "")
{
  BtreeCursorFixture f(true);
  f.cloneTest();
}

TEST_CASE("BtreeCursor-inmem/moveTest", "")
{
  BtreeCursorFixture f(true);
  f.moveTest();
}

TEST_CASE("BtreeCursor-inmem/moveSplitTest", "")
{
  BtreeCursorFixture f(true);
  f.moveSplitTest();
}

TEST_CASE("BtreeCursor-inmem/overwriteTest", "")
{
  BtreeCursorFixture f(true);
  f.overwriteTest();
}

TEST_CASE("BtreeCursor-inmem/linkedListTest", "")
{
  BtreeCursorFixture f(true);
  f.linkedListTest();
}

TEST_CASE("BtreeCursor-inmem/linkedListReverseCloseTest", "")
{
  BtreeCursorFixture f(true);
  f.linkedListReverseCloseTest();
}

TEST_CASE("BtreeCursor-inmem/cursorGetErasedItemTest", "")
{
  BtreeCursorFixture f(true);
  f.cursorGetErasedItemTest();
}

TEST_CASE("BtreeCursor-inmem/couplingTest", "")
{
  BtreeCursorFixture f(true);
  f.couplingTest();
}


TEST_CASE("BtreeCursor-64k-inmem/createCloseTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.createCloseTest();
}

TEST_CASE("BtreeCursor-64k-inmem/cloneTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.cloneTest();
}

TEST_CASE("BtreeCursor-64k-inmem/moveTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.moveTest();
}

TEST_CASE("BtreeCursor-64k-inmem/moveSplitTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.moveSplitTest();
}

TEST_CASE("BtreeCursor-64k-inmem/overwriteTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.overwriteTest();
}

TEST_CASE("BtreeCursor-64k-inmem/linkedListTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.linkedListTest();
}

TEST_CASE("BtreeCursor-64k-inmem/linkedListReverseCloseTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.linkedListReverseCloseTest();
}

TEST_CASE("BtreeCursor-64k-inmem/cursorGetErasedItemTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.cursorGetErasedItemTest();
}

TEST_CASE("BtreeCursor-64k-inmem/couplingTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.couplingTest();
}

} // namespace hamsterdb
