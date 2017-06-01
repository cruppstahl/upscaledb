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
#include "4context/context.h"

#include "os.hpp"
#include "fixture.hpp"


namespace upscaledb {

struct BtreeCursorFixture : BaseFixture {
  bool inmemory;
  ScopedPtr<Context> context;

  BtreeCursorFixture(bool inmemory_ = false, uint32_t page_size = 0)
    : inmemory(inmemory_) {
    ups_parameter_t params[] = {
      // set page_size, otherwise 16-bit limit bugs in freelist
      // will fire on Win32
      { UPS_PARAM_PAGESIZE, (page_size ? page_size : 4096) },
      { 0, 0 }
    };

    require_create(inmemory ? UPS_IN_MEMORY : 0, params,
                    UPS_ENABLE_DUPLICATE_KEYS, 0);
    context.reset(new Context(lenv(), 0, 0));
  }

  ~BtreeCursorFixture() {
    teardown();
  }

  void teardown() {
    context->changeset.clear();
    close();
  }

  void createCloseTest() {
    ups_cursor_t *c;

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));
    REQUIRE(0 == ups_cursor_close(c));
  }

  void cloneTest() {
    ups_cursor_t *cursor, *clone;

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(cursor != nullptr);
    LocalCursor *c = new LocalCursor(*(LocalCursor *)cursor);
    clone = (ups_cursor_t *)c;
    REQUIRE(clone != nullptr);
    REQUIRE(0 == ups_cursor_close(clone));
    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void overwriteTest() {
    ups_cursor_t *cursor;
    int x = 5;
    ups_key_t key = ups_make_key(&x, sizeof(x));
    ups_record_t rec = ups_make_record(&x, sizeof(x));

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_overwrite(cursor, &rec, 0));

    Page *page = btree_index()->root_page(context.get());;
    REQUIRE(page != nullptr);
    context->changeset.clear(); // unlock the pages
    BtreeCursor::uncouple_all_cursors(context.get(), page);

    REQUIRE(0 == ups_cursor_overwrite(cursor, &rec, 0));
  }

  void moveSplitTest() {
    ups_cursor_t *cursor, *cursor2, *cursor3;
    ups_parameter_t p1[] = {
      { UPS_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ups_parameter_t p2[] = {
      { UPS_PARAM_KEYSIZE, 70 },
      { 0, 0 }
    };

    teardown();
    require_create(inmemory ? UPS_IN_MEMORY : 0, p1, 0, p2);

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor2, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor3, db, 0, 0));

    char buffer[70] = {0};

    for (int i = 0; i < 64; i++) {
      *(int *)&buffer[0] = i;
      ups_key_t key = ups_make_key(&buffer[0], sizeof(buffer));
      ups_record_t rec = ups_make_record(&buffer[0], sizeof(buffer));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    }

    ups_key_t key = {0};
    ups_record_t rec = {0};
    REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_FIRST));
    REQUIRE(0 == *(int *)key.data);
    REQUIRE(0 == *(int *)rec.data);
    REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(63 == *(int *)key.data);
    REQUIRE(63 == *(int *)rec.data);

    for (int i = 0; i < 64; i++) {
      REQUIRE(0 == ups_cursor_move(cursor2, &key, &rec, UPS_CURSOR_NEXT));
      REQUIRE(i == *(int *)key.data);
      REQUIRE(i == *(int *)rec.data);
    }
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(cursor2, 0, 0, UPS_CURSOR_NEXT));
    for (int i = 63; i >= 0; i--) {
      REQUIRE(0 == ups_cursor_move(cursor3, &key, &rec, UPS_CURSOR_PREVIOUS));
      REQUIRE(i == *(int *)key.data);
      REQUIRE(i == *(int *)rec.data);
    }
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(cursor3, 0, 0, UPS_CURSOR_PREVIOUS));

    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_cursor_close(cursor2));
    REQUIRE(0 == ups_cursor_close(cursor3));
  }

  void moveTest() {
    ups_cursor_t *cursor;

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    /* no move, and cursor is nil: returns 0 if key/rec is 0 */
    REQUIRE(0 == ups_cursor_move(cursor, 0, 0, 0));

    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_LAST));
    REQUIRE(UPS_KEY_NOT_FOUND ==
          ups_cursor_move(cursor, 0, 0, UPS_CURSOR_PREVIOUS));

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void linkedListTest() {
    ups_cursor_t *cursor[5], *clone;

    REQUIRE((Cursor *)0 == ((LocalDb *)db)->cursor_list);

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_cursor_create(&cursor[i], db, 0, 0));
      REQUIRE((Cursor *)cursor[i] == ldb()->cursor_list);
    }

    REQUIRE(0 == ups_cursor_clone(cursor[0], &clone));
    REQUIRE(clone != nullptr);
    REQUIRE((Cursor *)clone == ldb()->cursor_list);

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_cursor_close(cursor[i]));
    }
    REQUIRE(0 == ups_cursor_close(clone));

    REQUIRE((Cursor *)0 == ldb()->cursor_list);
  }

  void linkedListReverseCloseTest() {
    ups_cursor_t *cursor[5], *clone;

    REQUIRE((Cursor *)0 == ldb()->cursor_list);

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_cursor_create(&cursor[i], db, 0, 0));
      REQUIRE(cursor[i] != nullptr);
      REQUIRE((Cursor *)cursor[i] == ldb()->cursor_list);
    }

    REQUIRE(0 == ups_cursor_clone(cursor[0], &clone));
    REQUIRE(clone != 0);
    REQUIRE((Cursor *)clone == ldb()->cursor_list);

    for (int i = 4; i >= 0; i--) {
      REQUIRE(0 == ups_cursor_close(cursor[i]));
    }
    REQUIRE(0 == ups_cursor_close(clone));

    REQUIRE((Cursor *)0 == ldb()->cursor_list);
  }

  void cursorGetErasedItemTest() {
    ups_cursor_t *cursor, *cursor2;
    int value = 0;
    ups_key_t key = ups_make_key(&value, sizeof(value));
    ups_record_t rec = {0};

    value = 1;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    value = 2;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor2, db, 0, 0));
    value = 1;
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
    REQUIRE(UPS_CURSOR_IS_NIL == ups_cursor_move(cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_move(cursor, &key, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 == ups_cursor_move(cursor2, &key, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 == ups_cursor_erase(cursor, 0));
    REQUIRE(UPS_CURSOR_IS_NIL == ups_cursor_move(cursor2, &key, 0, 0));
  }

  void couplingTest() {
    ups_cursor_t *c, *clone;
    int v1 = 1, v2 = 2, v3 = 3;
    ups_key_t key1 = ups_make_key(&v1, sizeof(v1));
    ups_key_t key2 = ups_make_key(&v2, sizeof(v2));
    ups_key_t key3 = ups_make_key(&v3, sizeof(v3));
    ups_record_t rec = {0};

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));
    BtreeCursor *btc = &((LocalCursor *)c)->btree_cursor;
    /* after create: cursor is NIL */
    REQUIRE(btc->is_coupled() == false);
    REQUIRE(btc->is_uncoupled() == false);

    /* after insert: cursor is NIL */
    REQUIRE(0 == ups_db_insert(db, 0, &key2, &rec, 0));
    REQUIRE(btc->is_coupled() == false);
    REQUIRE(btc->is_uncoupled() == false);

    /* move to item: cursor is coupled */
    REQUIRE(0 == ups_cursor_find(c, &key2, 0, 0));
    REQUIRE(btc->is_coupled() == true);
    REQUIRE(btc->is_uncoupled() == false);

    /* clone the coupled cursor */
    REQUIRE(0 == ups_cursor_clone(c, &clone));
    REQUIRE(0 == ups_cursor_close(clone));

    /* insert item BEFORE the first item - cursor is uncoupled */
    REQUIRE(0 == ups_db_insert(db, 0, &key1, &rec, 0));
    REQUIRE(btc->is_coupled() == false);
    REQUIRE(btc->is_uncoupled() == true);

    /* move to item: cursor is coupled */
    REQUIRE(0 == ups_cursor_find(c, &key2, 0, 0));
    REQUIRE(btc->is_coupled() == true);
    REQUIRE(btc->is_uncoupled() == false);

    /* insert duplicate - cursor stays coupled */
    REQUIRE(0 == ups_db_insert(db, 0, &key2, &rec, UPS_DUPLICATE));
    REQUIRE(btc->is_coupled() == true);
    REQUIRE(btc->is_uncoupled() == false);

    /* insert item AFTER the middle item - cursor stays coupled */
    REQUIRE(0 == ups_db_insert(db, 0, &key3, &rec, 0));
    REQUIRE(btc->is_coupled() == true);
    REQUIRE(btc->is_uncoupled() == false);
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


TEST_CASE("BtreeCursor/64k/createCloseTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.createCloseTest();
}

TEST_CASE("BtreeCursor/64k/cloneTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.cloneTest();
}

TEST_CASE("BtreeCursor/64k/moveTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.moveTest();
}

TEST_CASE("BtreeCursor/64k/moveSplitTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.moveSplitTest();
}

TEST_CASE("BtreeCursor/64k/overwriteTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.overwriteTest();
}

TEST_CASE("BtreeCursor/64k/linkedListTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.linkedListTest();
}

TEST_CASE("BtreeCursor/64k/linkedListReverseCloseTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.linkedListReverseCloseTest();
}

TEST_CASE("BtreeCursor/64k/cursorGetErasedItemTest", "")
{
  BtreeCursorFixture f(false, 1024 * 64);
  f.cursorGetErasedItemTest();
}

TEST_CASE("BtreeCursor/64k/couplingTest", "")
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


TEST_CASE("BtreeCursor/64k-inmem/createCloseTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.createCloseTest();
}

TEST_CASE("BtreeCursor/64k-inmem/cloneTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.cloneTest();
}

TEST_CASE("BtreeCursor/64k-inmem/moveTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.moveTest();
}

TEST_CASE("BtreeCursor/64k-inmem/moveSplitTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.moveSplitTest();
}

TEST_CASE("BtreeCursor/64k-inmem/overwriteTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.overwriteTest();
}

TEST_CASE("BtreeCursor/64k-inmem/linkedListTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.linkedListTest();
}

TEST_CASE("BtreeCursor/64k-inmem/linkedListReverseCloseTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.linkedListReverseCloseTest();
}

TEST_CASE("BtreeCursor/64k-inmem/cursorGetErasedItemTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.cursorGetErasedItemTest();
}

TEST_CASE("BtreeCursor/64k-inmem/couplingTest", "")
{
  BtreeCursorFixture f(true, 1024 * 64);
  f.couplingTest();
}

} // namespace upscaledb
