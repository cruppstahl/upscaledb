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

#include <limits>

#include "4context/context.h"

#include "os.hpp"
#include "fixture.hpp"

namespace upscaledb {

template<typename RecnoType>
struct RecordNumberFixture : BaseFixture {
  uint32_t m_flags;
  ScopedPtr<Context> context;

  RecordNumberFixture(uint32_t flags = 0)
      : m_flags(flags) {
    if (sizeof(RecnoType) == 4)
      require_create(m_flags, nullptr, UPS_RECORD_NUMBER32, nullptr);
    else
      require_create(m_flags, nullptr, UPS_RECORD_NUMBER64, nullptr);
    context.reset(new Context(lenv(), 0, 0));
  }

  ~RecordNumberFixture() {
    teardown();
  }

  void teardown() {
    context->changeset.clear();
    close();
  }

  void reopen() {
    teardown();
    require_open(m_flags);
  }

  void createCloseTest() {
    // nop
  }

  void createCloseOpenCloseTest() {
    reopen();

    uint32_t mask = UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64;
    REQUIRE((ldb()->flags() & mask) != 0);
  }

  void createInsertCloseReopenTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno, value = 1;

    key.flags = UPS_KEY_USER_ALLOC;
    key.data = &recno;
    key.size = sizeof(recno);

    rec.data = &value;
    rec.size = sizeof(value);

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }

    key.flags = UPS_KEY_USER_ALLOC;
    key.data = 0;
    REQUIRE(UPS_INV_PARAMETER == ups_db_insert(db, 0, &key, &rec, 0));
    key.data = &recno;
    key.size = sizeof(RecnoType) == 4 ? 8 : 4;
    REQUIRE(UPS_INV_KEY_SIZE == ups_db_insert(db, 0, &key, &rec, 0));
    key.size = sizeof(recno);

    key.flags = 0;
    key.size = 0;
    REQUIRE(UPS_INV_PARAMETER == ups_db_insert(db, 0, &key, &rec, 0));
    key.size = 8;
    key.data = 0;
    REQUIRE(UPS_INV_PARAMETER == ups_db_insert(db, 0, &key, &rec, 0));
    key.data = &recno;
    key.size = sizeof(RecnoType);
    key.flags = UPS_KEY_USER_ALLOC;

    reopen();

    for (int i = 5; i < 10; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }
  }

  void createInsertCloseReopenCursorTest(void)
  {
    ups_key_t key = {};
    ups_record_t rec = {};
    ups_cursor_t *cursor;
    RecnoType recno, value = 1;

    key.flags = UPS_KEY_USER_ALLOC;
    key.data = &recno;
    key.size = sizeof(recno);

    rec.data = &value;
    rec.size = sizeof(value);

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }

    REQUIRE(0 == ups_cursor_close(cursor));
    reopen();
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    for (int i = 5; i < 10; i++) {
      REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void createInsertCloseTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno, value = 1;

    key.flags = UPS_KEY_USER_ALLOC;
    key.data = &recno;
    key.size = sizeof(recno);

    rec.data = &value;
    rec.size = sizeof(value);

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }
  }

  void createInsertManyCloseTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno, value = 1;

    key.flags = UPS_KEY_USER_ALLOC;
    key.data = &recno;
    key.size = sizeof(recno);

    rec.data = &value;
    rec.size = sizeof(value);

    for (int i = 0; i < 500; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }

    key.size = sizeof(RecnoType) == 4 ? 8 : 4;
    REQUIRE(UPS_INV_KEY_SIZE == ups_db_find(db, 0, &key, &rec, 0));
    key.size = 0;
    key.data = &key;
    REQUIRE(UPS_INV_KEY_SIZE == ups_db_find(db, 0, &key, &rec, 0));

    for (int i = 0; i < 500; i++) {
      recno = i + 1;
      memset(&key, 0, sizeof(key));
      memset(&rec, 0, sizeof(rec));
      key.data = &recno;
      key.size = sizeof(recno);
      REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
    }
  }

  void createInsertCloseCursorTest(void) {
    ups_key_t key = {};
    ups_record_t rec = {};
    ups_cursor_t *cursor;
    RecnoType recno, value = 1;

    key.flags = UPS_KEY_USER_ALLOC;
    key.data = &recno;
    key.size = sizeof(recno);

    rec.data = &value;
    rec.size = sizeof(value);

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void createInsertCloseReopenTwiceTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno, value = 1;

    key.flags = UPS_KEY_USER_ALLOC;
    key.data = &recno;
    key.size = sizeof(recno);

    rec.data = &value;
    rec.size = sizeof(value);

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }

    reopen();

    for (int i = 5; i < 10; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }

    reopen();

    for (int i = 10; i < 15; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }
  }

  void createInsertCloseReopenTwiceCursorTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno, value = 1;
    ups_cursor_t *cursor;

    key.flags = UPS_KEY_USER_ALLOC;
    key.data = &recno;
    key.size = sizeof(recno);

    rec.data = &value;
    rec.size = sizeof(value);

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }

    REQUIRE(0 == ups_cursor_close(cursor));

    reopen();

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    for (int i = 5; i < 10; i++) {
      REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }

    REQUIRE(0 == ups_cursor_close(cursor));
    reopen();
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    for (int i = 10; i < 15; i++) {
      REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void insertBadKeyTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno;

    key.flags = 0;
    key.data = &recno;
    key.size = sizeof(recno);
    REQUIRE(UPS_INV_PARAMETER == ups_db_insert(db, 0, &key, &rec, 0));

    key.data = 0;
    key.size = 8;
    REQUIRE(UPS_INV_PARAMETER == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_db_insert(db, 0, 0, &rec, 0));

    key.data = 0;
    key.size = 0;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE((RecnoType)1ull == *(RecnoType *)key.data);
  }

  void insertBadKeyCursorTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    ups_cursor_t *cursor;
    RecnoType recno;

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    key.flags = 0;
    key.data = &recno;
    key.size = sizeof(recno);
    REQUIRE(UPS_INV_PARAMETER == ups_cursor_insert(cursor, &key, &rec, 0));

    key.data = 0;
    key.size = sizeof(RecnoType);
    REQUIRE(UPS_INV_PARAMETER == ups_cursor_insert(cursor, &key, &rec, 0));

    REQUIRE(UPS_INV_PARAMETER == ups_cursor_insert(cursor, 0, &rec, 0));

    key.data = 0;
    key.size = 0;
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE((RecnoType)1ull == *(RecnoType *)key.data);

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void createBadKeysizeTest() {
    ups_parameter_t p[] = {
      { UPS_PARAM_KEYSIZE, 7 },
      { 0, 0 }
    };

    REQUIRE(UPS_INV_KEYSIZE ==
        ups_env_create_db(env, &db, 2, UPS_RECORD_NUMBER32, &p[0]));
    REQUIRE(UPS_INV_KEYSIZE ==
        ups_env_create_db(env, &db, 2, UPS_RECORD_NUMBER64, &p[0]));

    p[0].value = 9;
    REQUIRE(UPS_INV_KEYSIZE ==
        ups_env_create_db(env, &db, 2, UPS_RECORD_NUMBER32, &p[0]));
    REQUIRE(UPS_INV_KEYSIZE ==
        ups_env_create_db(env, &db, 3, UPS_RECORD_NUMBER64, &p[0]));
  }

  void envTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno;

    key.data = &recno;
    key.size = sizeof(recno);
    key.flags = UPS_KEY_USER_ALLOC;

    teardown();
    if (sizeof(RecnoType) == 4)
      require_create(m_flags, nullptr, UPS_RECORD_NUMBER32, nullptr);
    else
      require_create(m_flags, nullptr, UPS_RECORD_NUMBER64, nullptr);

    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE((RecnoType)1ull == *(RecnoType *)key.data);

    if (!is_in_memory()) {
      reopen();
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      REQUIRE((RecnoType)2ull == *(RecnoType *)key.data);
    }
  }

  void overwriteTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno, value;

    key.data = &recno;
    key.flags = UPS_KEY_USER_ALLOC;
    key.size = sizeof(recno);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    value = 0x13ull;
    memset(&rec, 0, sizeof(rec));
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));

    key.size = sizeof(RecnoType) == 4 ? 8 : 4;
    REQUIRE(UPS_INV_KEY_SIZE ==
        ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    key.size = 8;
    key.data = 0;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    key.data = &recno;
    key.size = sizeof(RecnoType);

    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));

    REQUIRE(value == *(RecnoType *)rec.data);
  }

  void overwriteCursorTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno, value;
    ups_cursor_t *cursor;

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    key.data = &recno;
    key.flags = UPS_KEY_USER_ALLOC;
    key.size = sizeof(recno);
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));

    value = 0x13ull;
    memset(&rec, 0, sizeof(rec));
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, UPS_OVERWRITE));

    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));

    REQUIRE(value == *(RecnoType *)rec.data);

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void eraseLastReopenTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno;

    key.data = &recno;
    key.flags = UPS_KEY_USER_ALLOC;
    key.size = sizeof(recno);

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }

    REQUIRE(0 == ups_db_erase(db, 0, &key, 0));

    reopen();

    for (int i = 5; i < 10; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      REQUIRE((RecnoType)i == recno);
    }
  }

  void uncoupleTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno;
    ups_cursor_t *cursor, *c2;

    key.flags = UPS_KEY_USER_ALLOC;
    key.data = &recno;
    key.size = sizeof(recno);

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c2, db, 0, 0));

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }

    Page *page = btree_index()->root_page(context.get());
    REQUIRE(page != 0);
    context->changeset.clear(); // unlock pages
    BtreeCursor::uncouple_all_cursors(context.get(), page, 0);

    for (int i = 0; i < 5; i++) {
      REQUIRE(0 == ups_cursor_move(c2, &key, &rec, UPS_CURSOR_NEXT));
      REQUIRE(recno == (RecnoType)i + 1);
    }
  }

  void splitTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno;

    key.flags = UPS_KEY_USER_ALLOC;
    key.data = &recno;
    key.size = sizeof(recno);

    for (int i = 0; i < 4096; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      REQUIRE(recno == (RecnoType)i + 1);
    }
  }

  void overflowTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    RecnoType recno = std::numeric_limits<RecnoType>::max();
    ldb()->_current_record_number = recno;

    recno = 0;
    key.flags = UPS_KEY_USER_ALLOC;
    key.data = &recno;
    key.size = sizeof(recno);

    REQUIRE(UPS_LIMITS_REACHED == ups_db_insert(db, 0, &key, &rec, 0));
  }
};

TEST_CASE("RecordNumber64/createCloseTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.createCloseTest();
}

TEST_CASE("RecordNumber64/createCloseOpenCloseTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.createCloseOpenCloseTest();
}

TEST_CASE("RecordNumber64/createInsertCloseTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.createInsertCloseTest();
}

TEST_CASE("RecordNumber64/createInsertManyCloseTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.createInsertManyCloseTest();
}

TEST_CASE("RecordNumber64/createInsertCloseCursorTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.createInsertCloseCursorTest();
}

TEST_CASE("RecordNumber64/createInsertCloseReopenTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.createInsertCloseReopenTest();
}

TEST_CASE("RecordNumber64/createInsertCloseReopenCursorTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.createInsertCloseReopenCursorTest();
}

TEST_CASE("RecordNumber64/createInsertCloseReopenTwiceTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.createInsertCloseReopenTwiceTest();
}

TEST_CASE("RecordNumber64/createInsertCloseReopenTwiceCursorTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.createInsertCloseReopenTwiceCursorTest();
}

TEST_CASE("RecordNumber64/insertBadKeyTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.insertBadKeyTest();
}

TEST_CASE("RecordNumber64/insertBadKeyCursorTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.insertBadKeyCursorTest();
}

TEST_CASE("RecordNumber64/createBadKeysizeTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.createBadKeysizeTest();
}

TEST_CASE("RecordNumber64/envTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.envTest();
}

TEST_CASE("RecordNumber64/overwriteTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.overwriteTest();
}

TEST_CASE("RecordNumber64/overwriteCursorTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.overwriteCursorTest();
}

TEST_CASE("RecordNumber64/eraseLastReopenTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.eraseLastReopenTest();
}

TEST_CASE("RecordNumber64/uncoupleTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.uncoupleTest();
}

TEST_CASE("RecordNumber64/splitTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.splitTest();
}


TEST_CASE("RecordNumber64-inmem/createCloseTest", "")
{
  RecordNumberFixture<uint64_t> f(UPS_IN_MEMORY);
  f.createCloseTest();
}

TEST_CASE("RecordNumber64-inmem/createInsertCloseTest", "")
{
  RecordNumberFixture<uint64_t> f(UPS_IN_MEMORY);
  f.createInsertCloseTest();
}

TEST_CASE("RecordNumber64-inmem/createInsertManyCloseTest", "")
{
  RecordNumberFixture<uint64_t> f(UPS_IN_MEMORY);
  f.createInsertManyCloseTest();
}

TEST_CASE("RecordNumber64-inmem/createInsertCloseCursorTest", "")
{
  RecordNumberFixture<uint64_t> f(UPS_IN_MEMORY);
  f.createInsertCloseCursorTest();
}

TEST_CASE("RecordNumber64-inmem/insertBadKeyTest", "")
{
  RecordNumberFixture<uint64_t> f(UPS_IN_MEMORY);
  f.insertBadKeyTest();
}

TEST_CASE("RecordNumber64-inmem/insertBadKeyCursorTest", "")
{
  RecordNumberFixture<uint64_t> f(UPS_IN_MEMORY);
  f.insertBadKeyCursorTest();
}

TEST_CASE("RecordNumber64-inmem/createBadKeysizeTest", "")
{
  RecordNumberFixture<uint64_t> f(UPS_IN_MEMORY);
  f.createBadKeysizeTest();
}

TEST_CASE("RecordNumber64-inmem/envTest", "")
{
  RecordNumberFixture<uint64_t> f(UPS_IN_MEMORY);
  f.envTest();
}

TEST_CASE("RecordNumber64-inmem/overwriteTest", "")
{
  RecordNumberFixture<uint64_t> f(UPS_IN_MEMORY);
  f.overwriteTest();
}

TEST_CASE("RecordNumber64-inmem/overwriteCursorTest", "")
{
  RecordNumberFixture<uint64_t> f(UPS_IN_MEMORY);
  f.overwriteCursorTest();
}

TEST_CASE("RecordNumber64-inmem/uncoupleTest", "")
{
  RecordNumberFixture<uint64_t> f(UPS_IN_MEMORY);
  f.uncoupleTest();
}

TEST_CASE("RecordNumber64-inmem/splitTest", "")
{
  RecordNumberFixture<uint64_t> f(UPS_IN_MEMORY);
  f.splitTest();
}

TEST_CASE("RecordNumber32/createCloseTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.createCloseTest();
}

TEST_CASE("RecordNumber32/createCloseOpenCloseTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.createCloseOpenCloseTest();
}

TEST_CASE("RecordNumber32/createInsertCloseTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.createInsertCloseTest();
}

TEST_CASE("RecordNumber32/createInsertManyCloseTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.createInsertManyCloseTest();
}

TEST_CASE("RecordNumber32/createInsertCloseCursorTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.createInsertCloseCursorTest();
}

TEST_CASE("RecordNumber32/createInsertCloseReopenTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.createInsertCloseReopenTest();
}

TEST_CASE("RecordNumber32/createInsertCloseReopenCursorTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.createInsertCloseReopenCursorTest();
}

TEST_CASE("RecordNumber32/createInsertCloseReopenTwiceTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.createInsertCloseReopenTwiceTest();
}

TEST_CASE("RecordNumber32/createInsertCloseReopenTwiceCursorTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.createInsertCloseReopenTwiceCursorTest();
}

TEST_CASE("RecordNumber32/insertBadKeyTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.insertBadKeyTest();
}

TEST_CASE("RecordNumber32/insertBadKeyCursorTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.insertBadKeyCursorTest();
}

TEST_CASE("RecordNumber32/createBadKeysizeTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.createBadKeysizeTest();
}

TEST_CASE("RecordNumber32/envTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.envTest();
}

TEST_CASE("RecordNumber32/overwriteTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.overwriteTest();
}

TEST_CASE("RecordNumber32/overwriteCursorTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.overwriteCursorTest();
}

TEST_CASE("RecordNumber32/eraseLastReopenTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.eraseLastReopenTest();
}

TEST_CASE("RecordNumber32/uncoupleTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.uncoupleTest();
}

TEST_CASE("RecordNumber32/splitTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.splitTest();
}

TEST_CASE("RecordNumber32-inmem/createCloseTest", "")
{
  RecordNumberFixture<uint32_t> f(UPS_IN_MEMORY);
  f.createCloseTest();
}

TEST_CASE("RecordNumber32-inmem/createInsertCloseTest", "")
{
  RecordNumberFixture<uint32_t> f(UPS_IN_MEMORY);
  f.createInsertCloseTest();
}

TEST_CASE("RecordNumber32-inmem/createInsertManyCloseTest", "")
{
  RecordNumberFixture<uint32_t> f(UPS_IN_MEMORY);
  f.createInsertManyCloseTest();
}

TEST_CASE("RecordNumber32-inmem/createInsertCloseCursorTest", "")
{
  RecordNumberFixture<uint32_t> f(UPS_IN_MEMORY);
  f.createInsertCloseCursorTest();
}

TEST_CASE("RecordNumber32-inmem/insertBadKeyTest", "")
{
  RecordNumberFixture<uint32_t> f(UPS_IN_MEMORY);
  f.insertBadKeyTest();
}

TEST_CASE("RecordNumber32-inmem/insertBadKeyCursorTest", "")
{
  RecordNumberFixture<uint32_t> f(UPS_IN_MEMORY);
  f.insertBadKeyCursorTest();
}

TEST_CASE("RecordNumber32-inmem/createBadKeysizeTest", "")
{
  RecordNumberFixture<uint32_t> f(UPS_IN_MEMORY);
  f.createBadKeysizeTest();
}

TEST_CASE("RecordNumber32-inmem/envTest", "")
{
  RecordNumberFixture<uint32_t> f(UPS_IN_MEMORY);
  f.envTest();
}

TEST_CASE("RecordNumber32-inmem/overwriteTest", "")
{
  RecordNumberFixture<uint32_t> f(UPS_IN_MEMORY);
  f.overwriteTest();
}

TEST_CASE("RecordNumber32-inmem/overwriteCursorTest", "")
{
  RecordNumberFixture<uint32_t> f(UPS_IN_MEMORY);
  f.overwriteCursorTest();
}

TEST_CASE("RecordNumber32-inmem/uncoupleTest", "")
{
  RecordNumberFixture<uint32_t> f(UPS_IN_MEMORY);
  f.uncoupleTest();
}

TEST_CASE("RecordNumber32-inmem/splitTest", "")
{
  RecordNumberFixture<uint32_t> f(UPS_IN_MEMORY);
  f.splitTest();
}

TEST_CASE("RecordNumber64/overflowTest", "")
{
  RecordNumberFixture<uint64_t> f;
  f.overflowTest();
}

TEST_CASE("RecordNumber32/overflowTest", "")
{
  RecordNumberFixture<uint32_t> f;
  f.overflowTest();
}

} // namespace upscaledb
