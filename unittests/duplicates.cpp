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

#include <vector>
#include <string>

#include "3rdparty/catch/catch.hpp"

#include "4context/context.h"
#include "4cursor/cursor_local.h"

#include "os.hpp"
#include "fixture.hpp"

namespace upscaledb {

struct DuplicateFixture : BaseFixture {
  uint32_t m_flags;
  ScopedPtr<Context> context;

  DuplicateFixture(uint32_t flags = 0)
    : m_flags(flags) {
    require_create(m_flags, nullptr, UPS_ENABLE_DUPLICATE_KEYS, nullptr);
    context.reset(new Context(lenv(), 0, 0));
  }

  ~DuplicateFixture() {
    teardown();
  }

  void teardown() {
    context->changeset.clear();
    close();
  }

  void insertDuplicatesTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    ups_record_t rec2 = {};
    char data[16];

    for (int i = 0; i < 10; i++) {
      rec.data = data;
      rec.size = sizeof(data);
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
          ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));
    }

    ::memset(&data, 0x15, sizeof(data));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE((uint32_t)sizeof(data) == rec2.size);
    REQUIRE(0 == ::memcmp(data, rec2.data, sizeof(data)));
  }

  void insertDuplicatesFirstTest() {
    ups_parameter_t params[] = {
      {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT64},
      {UPS_PARAM_RECORD_SIZE, 10},
      {0, 0}
    };

    teardown();
    require_create(0, nullptr, UPS_ENABLE_DUPLICATE_KEYS, &params[0]);

    ups_key_t key = {};
    ups_record_t rec = {};
    ups_record_t rec2 = {};
    char data[10];

    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    uint64_t k = 0;
    key.data = &k;
    key.size = sizeof(k);
    rec.data = data;
    rec.size = sizeof(data);
    int i;
    for (i = 0; i < 10; i++) {
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
            ups_cursor_insert(cursor, &key, &rec, UPS_DUPLICATE_INSERT_FIRST));
    }

    REQUIRE(0 ==
        ups_cursor_move(cursor, &key, &rec2, UPS_CURSOR_FIRST));
    for (i--; i >= 0; i--) {
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(sizeof(k) == key.size);
      REQUIRE(*(uint64_t *)key.data == k);
      REQUIRE((uint32_t)sizeof(data) == rec2.size);
      REQUIRE(0 == ::memcmp(data, rec2.data, sizeof(data)));

      if (i > 0)
        REQUIRE(0 ==
            ups_cursor_move(cursor, &key, &rec2, UPS_CURSOR_NEXT));
    }
  }

  void overwriteDuplicatesTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    ups_record_t rec2 = {};
    ups_cursor_t *c;
    uint32_t count;
    char data[16];

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    for (int i = 0; i < 5; i++) {
      rec.data = data;
      rec.size = sizeof(data);
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
          ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));
    }

    rec.data = data;
    rec.size = sizeof(data);
    ::memset(&data, 0x99, sizeof(data));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));

    REQUIRE(0 == ups_cursor_move(c, &key, &rec2, UPS_CURSOR_FIRST));
    REQUIRE((uint32_t)sizeof(data) == rec2.size);
    REQUIRE(0 == ::memcmp(data, rec2.data, sizeof(data)));

    for (int i = 1; i < 5; i++) {
      ::memset(&data, i+0x15, sizeof(data));
      REQUIRE(0 ==
          ups_cursor_move(c, &key, &rec2, UPS_CURSOR_NEXT));
      REQUIRE((uint32_t)sizeof(data) == rec2.size);
      REQUIRE(0 == ::memcmp(data, rec2.data, sizeof(data)));
    }

    REQUIRE(0 ==
        ups_cursor_move(c, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 ==
        ups_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((uint32_t)5 == count);

    REQUIRE(0 == ups_cursor_close(c));
  }

  void overwriteVariousDuplicatesTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    ups_record_t rec2 = {};
#define M  10
    unsigned sizes[M] = { 0, 1, 2, 3, 4, 5, 936, 5, 100, 50 };
    char *data = 0;
    ups_cursor_t *cursor;
    uint32_t count;

    for (unsigned i = 0; i < M; i++) {
      data = 0;
      if (sizes[i]) {
        data = (char *)malloc(sizes[i]);
        ::memset(data, i + 0x15, sizes[i]);
      }
      rec.data = sizes[i] ? data : 0;
      rec.size = sizes[i];
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));
      REQUIRE(sizes[i] == rec.size);
      if (sizes[i]) {
        REQUIRE(0 == ::memcmp(data, rec.data, sizes[i]));
        free(data);
      }
    }

    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE((uint32_t)0 == rec2.size);

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    for (unsigned i = 0; i < M; i++) {
      if (sizes[i]) {
        data = (char *)malloc(sizes[i]);
        ::memset(data, i+0x15, sizes[i]);
      }
      rec.data = sizes[i] ? data : 0;
      rec.size = sizes[i];
      REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));
      REQUIRE(sizes[i] == rec.size);
      if (sizes[i]) {
        REQUIRE(0 == ::memcmp(data, rec.data, sizes[i]));
        free(data);
      }
    }

    REQUIRE(0 == ups_cursor_get_duplicate_count(cursor, &count, 0));
    REQUIRE((uint32_t)M == count);

    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    for (int i = M - 1; i >= 0; i--) {
      if (sizes[i]) {
        data = (char *)malloc(sizes[i]);
        ::memset(data, i+0x15, sizes[i]);
      }
      rec.data = sizes[i] ? data : 0;
      rec.size = sizes[i];
      REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_PREVIOUS));
      REQUIRE(sizes[i] == rec.size);
      if (sizes[i]) {
        REQUIRE(0 == ::memcmp(data, rec.data, sizes[i]));
        free(data);
      }
    }

    REQUIRE(0 == ups_cursor_get_duplicate_count(cursor, &count, 0));
    REQUIRE((uint32_t)M == count);

    REQUIRE(0 == ups_cursor_close(cursor));

    data = (char *)malloc(16);
    ::memset(data, 0x99, 16);
    rec.data = data;
    rec.size = 16;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));

    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE((uint32_t)16 == rec2.size);
    REQUIRE(0 == ::memcmp(data, rec2.data, 16));
    free(data);
  }

  void insertMoveForwardTest() {
    ups_key_t key;
    ups_record_t rec;
    ups_cursor_t *cursor;
    char data[16];

    for (int i = 0; i < 5; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      rec.data = data;
      rec.size = sizeof(data);
      ::memset(&data, i+0x15, sizeof(data));
      REQUIRE(0 ==
          ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));
    }

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    for (int i = 0; i < 5; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
          ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));
      REQUIRE((uint32_t)sizeof(data) == rec.size);
      REQUIRE(0 == ::memcmp(data, rec.data, sizeof(data)));
    }

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void insertMoveBackwardTest() {
    ups_key_t key;
    ups_record_t rec;
    ups_cursor_t *cursor;
    char data[16];

    for (int i = 0; i < 5; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      rec.data = data;
      rec.size = sizeof(data);
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
          ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));
    }

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    for (int i = 4; i >= 0; i--) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
          ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_PREVIOUS));
      REQUIRE((uint32_t)sizeof(data) == rec.size);
      REQUIRE(0 == ::memcmp(data, rec.data, sizeof(data)));
    }

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(UPS_KEY_NOT_FOUND ==
        ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_PREVIOUS));

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void insertEraseTest() {
    ups_key_t key;
    ups_record_t rec;
    char data[16];
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    ::memset(&data, 0x13, sizeof(data));
    rec.data = data;
    rec.size = sizeof(data);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    ::memset(&data, 0x14, sizeof(data));
    rec.data = data;
    rec.size = sizeof(data);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    ::memset(&data, 0x15, sizeof(data));
    rec.data = data;
    rec.size = sizeof(data);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    ::memset(&rec, 0, sizeof(rec));
    ::memset(&data, 0x13, sizeof(data));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE((uint32_t)sizeof(data) == rec.size);
    REQUIRE(0 == ::memcmp(data, rec.data, sizeof(data)));

    REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, 0, &key, &rec, 0));
  }

  void insert(ups_key_t *key, ups_record_t *rec) {
    REQUIRE(0 == ups_db_insert(db, 0, key, rec, UPS_DUPLICATE));
  }

  void find(ups_key_t *key, ups_record_t *rec) {
    ups_record_t record = {};

    REQUIRE(0 == ups_db_find(db, 0, key, &record, 0));
    REQUIRE(rec->size == record.size);
    REQUIRE(0 == ::memcmp(rec->data, record.data, rec->size));
  }

  void erase(ups_key_t *key) {
    REQUIRE(0 == ups_db_erase(db, 0, key, 0));
  }

  void insertData(const char *k, const char *data) {
    ups_key_t key = {};
    ups_record_t rec = {};
    rec.data = (void *)data;
    rec.size = (uint32_t)::strlen(data)+1;
    key.data = (void *)k;
    key.size = (uint16_t)(k ? ::strlen(k)+1 : 0);

    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));
  }

  void checkData(ups_cursor_t *cursor, uint32_t flags,
      ups_status_t expected, const char *data) {
    ups_key_t key = {};
    ups_record_t rec = {};

    REQUIRE(expected == ups_cursor_move(cursor, &key, &rec, flags));

    if (expected == 0) {
      if (data) {
        REQUIRE(rec.size == (uint32_t)::strlen(data) + 1);
        REQUIRE(0 == ::memcmp(rec.data, data, rec.size));
      }
      else {
        REQUIRE(rec.size == (uint32_t)0);
        REQUIRE((void *)0 == rec.data);
      }
    }
  }

  void insertTest() {
    ups_cursor_t *c;
    uint32_t count;

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    insertData(0, "1111111111");
    insertData(0, "2222222222");
    insertData(0, "3333333333");
    insertData(0, "4444444444");
    insertData(0, "5555555555");
    insertData(0, "6666666666");
    insertData(0, "7777777777");
    insertData(0, "8888888888");
    insertData(0, "9999999999");
    insertData(0, "0000000000");

    checkData(c, UPS_CURSOR_NEXT,   0, "1111111111");
    checkData(c, UPS_CURSOR_NEXT,   0, "2222222222");
    checkData(c, UPS_CURSOR_NEXT,   0, "3333333333");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "2222222222");
    checkData(c, UPS_CURSOR_NEXT,   0, "3333333333");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "2222222222");
    checkData(c, UPS_CURSOR_NEXT,   0, "3333333333");
    checkData(c, UPS_CURSOR_NEXT,   0, "4444444444");
    checkData(c, UPS_CURSOR_NEXT,   0, "5555555555");
    checkData(c, UPS_CURSOR_NEXT,   0, "6666666666");
    checkData(c, UPS_CURSOR_NEXT,   0, "7777777777");
    checkData(c, UPS_CURSOR_NEXT,   0, "8888888888");
    checkData(c, UPS_CURSOR_NEXT,   0, "9999999999");
    checkData(c, UPS_CURSOR_NEXT,   0, "0000000000");
    checkData(c, UPS_CURSOR_NEXT,   UPS_KEY_NOT_FOUND, "0000000000");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "9999999999");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "8888888888");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "7777777777");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "6666666666");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "5555555555");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "4444444444");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "3333333333");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "2222222222");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "1111111111");
    checkData(c, UPS_CURSOR_PREVIOUS, UPS_KEY_NOT_FOUND, "0000000000");
    checkData(c, UPS_CURSOR_NEXT,   0, "2222222222");
    checkData(c, UPS_CURSOR_NEXT,   0, "3333333333");

    REQUIRE(0 ==
        ups_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((uint32_t)10 == count);

    ups_cursor_close(c);
  }

  void insertSkipDuplicatesTest() {
    ups_cursor_t *c;

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    insertData("000", "aaaaaaaaaa");
    insertData("111", "1111111111");
    insertData("111", "2222222222");
    insertData("111", "3333333333");
    insertData("111", "4444444444");
    insertData("111", "5555555555");
    insertData("111", "6666666666");
    insertData("111", "7777777777");
    insertData("111", "8888888888");
    insertData("111", "9999999999");
    insertData("111", "0000000000");
    insertData("222", "bbbbbbbbbb");
    insertData("333", "cccccccccc");

    checkData(c, UPS_CURSOR_NEXT,   0, "aaaaaaaaaa");
    checkData(c, UPS_CURSOR_NEXT,   0, "1111111111");
    checkData(c, UPS_CURSOR_NEXT,   0, "2222222222");
    checkData(c, UPS_CURSOR_NEXT|UPS_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
    checkData(c, UPS_CURSOR_NEXT|UPS_SKIP_DUPLICATES, 0, "cccccccccc");
    checkData(c, UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
    checkData(c, UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES, 0, "1111111111");
    checkData(c, UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES, 0, "aaaaaaaaaa");

    ups_cursor_close(c);
  }

  void insertOnlyDuplicatesTest() {
    ups_cursor_t *c;

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    insertData("000", "aaaaaaaaaa");
    insertData("111", "8888888888");
    insertData("111", "9999999999");
    insertData("111", "0000000000");
    insertData("222", "bbbbbbbbbb");

    checkData(c, UPS_CURSOR_FIRST,  0, "aaaaaaaaaa");
    checkData(c, UPS_CURSOR_NEXT,   0, "8888888888");
    checkData(c, UPS_CURSOR_NEXT|UPS_ONLY_DUPLICATES, 0, "9999999999");
    checkData(c, UPS_CURSOR_NEXT|UPS_ONLY_DUPLICATES, 0, "0000000000");
    checkData(c, UPS_CURSOR_NEXT|UPS_ONLY_DUPLICATES, UPS_KEY_NOT_FOUND, 0);

    checkData(c, UPS_CURSOR_FIRST,  0, "aaaaaaaaaa");
    checkData(c, UPS_CURSOR_NEXT,   0, "8888888888");
    checkData(c, UPS_CURSOR_NEXT|UPS_ONLY_DUPLICATES, 0, "9999999999");
    checkData(c, UPS_CURSOR_PREVIOUS|UPS_ONLY_DUPLICATES, 0, "8888888888");
    checkData(c, UPS_CURSOR_PREVIOUS|UPS_ONLY_DUPLICATES,
            UPS_KEY_NOT_FOUND, 0);

    checkData(c, UPS_CURSOR_FIRST,  0, "aaaaaaaaaa");
    checkData(c, UPS_CURSOR_PREVIOUS|UPS_ONLY_DUPLICATES,
            UPS_KEY_NOT_FOUND, 0);

    ups_cursor_close(c);
  }

  void insertOnlyDuplicatesTest2() {
    ups_cursor_t *c;

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    insertData("1", "1");
    insertData("1", "2");
    insertData("1", "3");
    insertData("1", "4");

    const char *exp[] = { "1", "2", "3", "4" };

    ups_key_t key = {};
    key.data = (void *)"1";
    key.size = 2;
    ups_record_t rec = {};

    REQUIRE(0 == ups_cursor_find(c, &key, 0, 0));
    for (int i = 0; i < 3; i++) {
      REQUIRE(0 == ups_cursor_move(c, 0, &rec, 0));
      REQUIRE(0 == strcmp(exp[i], (char *)rec.data));
      REQUIRE(0 ==
          ups_cursor_move(c, &key, &rec,
              UPS_CURSOR_NEXT | UPS_ONLY_DUPLICATES));
    }

    checkData(c, UPS_CURSOR_NEXT | UPS_ONLY_DUPLICATES,
        UPS_KEY_NOT_FOUND, 0);

    ups_cursor_close(c);
  }

  void coupleUncoupleTest() {
    ups_cursor_t *c;
    Page *page;

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    insertData("000", "aaaaaaaaaa");
    insertData("111", "1111111111");
    insertData("111", "2222222222");
    insertData("111", "3333333333");
    insertData("111", "4444444444");
    insertData("111", "5555555555");
    insertData("111", "6666666666");
    insertData("111", "7777777777");
    insertData("111", "8888888888");
    insertData("111", "9999999999");
    insertData("111", "0000000000");
    insertData("222", "bbbbbbbbbb");
    insertData("333", "cccccccccc");

    REQUIRE((page = btree_index()->root_page(context.get())));
    context->changeset.clear(); // unlock pages

    BtreeCursor::uncouple_all_cursors(context.get(), page);
    checkData(c, UPS_CURSOR_NEXT,   0, "aaaaaaaaaa");
    BtreeCursor::uncouple_all_cursors(context.get(), page);
    BtreeCursor::uncouple_all_cursors(context.get(), page);
    checkData(c, UPS_CURSOR_NEXT,   0, "1111111111");
    BtreeCursor::uncouple_all_cursors(context.get(), page);
    checkData(c, UPS_CURSOR_NEXT,   0, "2222222222");
    BtreeCursor::uncouple_all_cursors(context.get(), page);
    checkData(c, UPS_CURSOR_NEXT | UPS_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
    BtreeCursor::uncouple_all_cursors(context.get(), page);
    checkData(c, UPS_CURSOR_NEXT | UPS_SKIP_DUPLICATES, 0, "cccccccccc");
    BtreeCursor::uncouple_all_cursors(context.get(), page);
    checkData(c, UPS_CURSOR_PREVIOUS | UPS_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
    BtreeCursor::uncouple_all_cursors(context.get(), page);
    checkData(c, UPS_CURSOR_PREVIOUS | UPS_SKIP_DUPLICATES, 0, "1111111111");
    BtreeCursor::uncouple_all_cursors(context.get(), page);
    checkData(c, UPS_CURSOR_PREVIOUS | UPS_SKIP_DUPLICATES, 0, "aaaaaaaaaa");

    ups_cursor_close(c);
  }

  void eraseOtherDuplicateUncoupledTest() {
    ups_cursor_t *c1, *c2;
    ups_key_t key = {};
    ups_record_t rec = {};
    int value = 1;

    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    REQUIRE(0 == ups_cursor_create(&c1, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c2, db, 0, 0));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ups_cursor_move(c1, &key, &rec, UPS_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ups_cursor_move(c2, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    ((LocalCursor *)c2)->btree_cursor.uncouple_from_page(context.get());
    REQUIRE(0 == ups_cursor_erase(c1, 0));
    REQUIRE(((LocalCursor *)c1)->is_nil(LocalCursor::kBtree));
    REQUIRE(!((LocalCursor *)c2)->is_nil(LocalCursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c1, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c2, &key, &rec, 0));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_close(c1));
    REQUIRE(0 == ups_cursor_close(c2));
  }

  void eraseMiddleDuplicateTest() {
    ups_cursor_t *c1, *c2;
    ups_key_t key;
    ups_record_t rec;
    int value = 0;
    ::memset(&key, 0, sizeof(key));

    ::memset(&rec, 0, sizeof(rec));
    value = 1;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    ::memset(&rec, 0, sizeof(rec));
    value=3;
    rec.data=&value;
    rec.size=sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    REQUIRE(0 == ups_cursor_create(&c1, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c2, db, 0, 0));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ups_cursor_move(c1, &key, &rec, UPS_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ups_cursor_move(c1, &key, &rec, UPS_CURSOR_NEXT));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ups_cursor_move(c2, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(3 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_erase(c1, 0));
    REQUIRE(((LocalCursor *)c1)->is_nil(LocalCursor::kBtree));
    REQUIRE(!((LocalCursor *)c2)->is_nil(LocalCursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c1, &key, &rec, UPS_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c1, &key, &rec, UPS_CURSOR_NEXT));
    REQUIRE(3 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c2, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(3 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c2, &key, &rec, UPS_CURSOR_PREVIOUS));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_close(c1));
    REQUIRE(0 == ups_cursor_close(c2));
  }

  void eraseTinyDuplicatesTest() {
    ups_cursor_t *c;

    insertData("111", "111");
    insertData("111", "222");
    insertData("111", "333");
    insertData("111", "444");
    insertData("111", "555");

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    checkData(c, UPS_CURSOR_FIRST,  0, "111");
    REQUIRE(0 == ups_cursor_erase(c, 0));
    checkData(c, UPS_CURSOR_FIRST,  0, "222");
    REQUIRE(0 == ups_cursor_erase(c, 0));
    checkData(c, UPS_CURSOR_FIRST,  0, "333");
    REQUIRE(0 == ups_cursor_erase(c, 0));
    checkData(c, UPS_CURSOR_FIRST,  0, "444");
    REQUIRE(0 == ups_cursor_erase(c, 0));
    checkData(c, UPS_CURSOR_FIRST,  0, "555");
    REQUIRE(0 == ups_cursor_erase(c, 0));
    checkData(c, UPS_CURSOR_FIRST,  UPS_KEY_NOT_FOUND, "555");

    REQUIRE(0 == ups_cursor_close(c));
  }

  void reopenTest() {
    ups_cursor_t *c;

    insertData("000", "aaaaaaaaaa");
    insertData("111", "1111111111");
    insertData("111", "2222222222");
    insertData("111", "3333333333");
    insertData("222", "bbbbbbbbbb");

    if (!is_in_memory()) {
      /* reopen the database */
      teardown();
      require_open(m_flags);
    }
    REQUIRE(ISSET(ldb()->flags(), UPS_ENABLE_DUPLICATE_KEYS));

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    insertData("111", "4444444444");
    insertData("111", "5555555555");
    insertData("111", "6666666666");

    checkData(c, UPS_CURSOR_FIRST,  0, "aaaaaaaaaa");
    checkData(c, UPS_CURSOR_NEXT,   0, "1111111111");
    checkData(c, UPS_CURSOR_NEXT,   0, "2222222222");
    checkData(c, UPS_CURSOR_NEXT,   0, "3333333333");
    checkData(c, UPS_CURSOR_NEXT,   0, "4444444444");
    checkData(c, UPS_CURSOR_NEXT,   0, "5555555555");
    checkData(c, UPS_CURSOR_NEXT,   0, "6666666666");
    checkData(c, UPS_CURSOR_NEXT,   0, "bbbbbbbbbb");

    checkData(c, UPS_CURSOR_NEXT|UPS_ONLY_DUPLICATES,
            UPS_KEY_NOT_FOUND, 0);
    checkData(c, UPS_CURSOR_NEXT,
            UPS_KEY_NOT_FOUND, 0);

    REQUIRE(0 == ups_cursor_close(c));
  }

  void moveToLastDuplicateTest() {
    ups_cursor_t *c;

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    insertData(0, "3333333333");
    insertData(0, "2222222222");
    insertData(0, "1111111111");

    checkData(c, UPS_CURSOR_LAST,   0, "1111111111");

    ups_cursor_close(c);
  }

  void eraseDuplicateTest() {
    ups_cursor_t *c1, *c2;
    ups_key_t key = {0};
    ups_record_t rec = {0};

    int value = 1;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    REQUIRE(0 == ups_cursor_create(&c1, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c2, db, 0, 0));

    REQUIRE(0 == ups_cursor_find(c1, &key, 0, 0));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ups_cursor_move(c1, &key, &rec, 0));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_find(c2, &key, 0, 0));
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ups_cursor_move(c2, &key, &rec, 0));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_erase(c1, 0));
    REQUIRE(((LocalCursor *)c1)->is_nil(LocalCursor::kBtree));
    REQUIRE(((LocalCursor *)c2)->is_nil(LocalCursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ups_cursor_move(c1, &key, &rec, UPS_CURSOR_FIRST));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ups_cursor_move(c2, &key, &rec, UPS_CURSOR_FIRST));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_close(c1));
    REQUIRE(0 == ups_cursor_close(c2));
  }

  void eraseDuplicateUncoupledTest() {
    ups_cursor_t *c1, *c2;
    ups_key_t key;
    ups_record_t rec;
    int value = 0;
    ::memset(&key, 0, sizeof(key));

    ::memset(&rec, 0, sizeof(rec));
    value = 1;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    REQUIRE(0 == ups_cursor_create(&c1, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c2, db, 0, 0));

    REQUIRE(0 == ups_cursor_find(c1, &key, 0, 0));
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c1, &key, &rec, 0));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_find(c2, &key, 0, 0));
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c2, &key, &rec, 0));
    REQUIRE(1 == *(int *)rec.data);

    ((LocalCursor *)c1)->btree_cursor.uncouple_from_page(context.get());
    ((LocalCursor *)c2)->btree_cursor.uncouple_from_page(context.get());
    REQUIRE(0 == ups_cursor_erase(c1, 0));
    REQUIRE(((LocalCursor *)c1)->is_nil(LocalCursor::kBtree));
    REQUIRE(((LocalCursor *)c2)->is_nil(LocalCursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c1, &key, &rec, UPS_CURSOR_FIRST));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c2, &key, &rec, UPS_CURSOR_FIRST));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_close(c1));
    REQUIRE(0 == ups_cursor_close(c2));
  }

  void eraseSecondDuplicateTest() {
    ups_cursor_t *c1, *c2;
    ups_key_t key;
    ups_record_t rec;
    int value = 1;
    ::memset(&key, 0, sizeof(key));

    ::memset(&rec, 0, sizeof(rec));
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    REQUIRE(0 == ups_cursor_create(&c1, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c2, db, 0, 0));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ups_cursor_move(c1, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ups_cursor_move(c2, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_erase(c1, 0));
    REQUIRE(((LocalCursor *)c1)->is_nil(LocalCursor::kBtree));
    REQUIRE(((LocalCursor *)c2)->is_nil(LocalCursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c1, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(1 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c2, &key, &rec, UPS_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_close(c1));
    REQUIRE(0 == ups_cursor_close(c2));
  }

  void eraseSecondDuplicateUncoupledTest() {
    ups_cursor_t *c1, *c2;
    ups_key_t key;
    ups_record_t rec;
    int value = 1;
    ::memset(&key, 0, sizeof(key));

    ::memset(&rec, 0, sizeof(rec));
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    REQUIRE(0 == ups_cursor_create(&c1, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c2, db, 0, 0));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ups_cursor_move(c1, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ups_cursor_move(c2, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    ((LocalCursor *)c1)->btree_cursor.uncouple_from_page(context.get());
    ((LocalCursor *)c2)->btree_cursor.uncouple_from_page(context.get());
    REQUIRE(0 == ups_cursor_erase(c1, 0));
    REQUIRE(((LocalCursor *)c1)->is_nil(LocalCursor::kBtree));
    REQUIRE(((LocalCursor *)c2)->is_nil(LocalCursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c1, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(1 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c2, &key, &rec, UPS_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_close(c1));
    REQUIRE(0 == ups_cursor_close(c2));
  }

  void eraseOtherDuplicateTest() {
    ups_cursor_t *c1, *c2;
    ups_key_t key;
    ups_record_t rec;
    int value = 1;
    ::memset(&key, 0, sizeof(key));

    ::memset(&rec, 0, sizeof(rec));
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    REQUIRE(0 == ups_cursor_create(&c1, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c2, db, 0, 0));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ups_cursor_move(c1, &key, &rec, UPS_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ups_cursor_move(c2, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_erase(c1, 0));
    REQUIRE(((LocalCursor *)c1)->is_nil(LocalCursor::kBtree));
    REQUIRE(!((LocalCursor *)c2)->is_nil(LocalCursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c1, &key, &rec, UPS_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ups_cursor_move(c2, &key, &rec, 0));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_close(c1));
    REQUIRE(0 == ups_cursor_close(c2));
  }

  void moveToPreviousDuplicateTest() {
    ups_cursor_t *c;

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    insertData(0, "1111111111");
    insertData(0, "2222222222");
    insertData(0, "3333333333");
    insertData(0, "4444444444");
    insertData(0, "5555555555");
    insertData(0, "6666666666");
    insertData(0, "7777777777");
    insertData(0, "8888888888");
    insertData(0, "9999999999");
    insertData(0, "0000000000");
    insertData("1", "xxxxxxxx");

    checkData(c, UPS_CURSOR_LAST,   0, "xxxxxxxx");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "0000000000");

    checkData(c, UPS_CURSOR_LAST,   0, "xxxxxxxx");
    checkData(c, UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES, 0, "1111111111");

    checkData(c, UPS_CURSOR_LAST,   0, "xxxxxxxx");
    checkData(c, UPS_CURSOR_PREVIOUS|UPS_ONLY_DUPLICATES,
        UPS_KEY_NOT_FOUND, 0);

    checkData(c, UPS_CURSOR_FIRST,  0, "1111111111");
    checkData(c, UPS_CURSOR_PREVIOUS|UPS_ONLY_DUPLICATES,
        UPS_KEY_NOT_FOUND, 0);
    checkData(c, UPS_CURSOR_PREVIOUS|UPS_SKIP_DUPLICATES,
        UPS_KEY_NOT_FOUND, 0);
    checkData(c, UPS_CURSOR_PREVIOUS,
        UPS_KEY_NOT_FOUND, 0);

    ups_cursor_close(c);
  }

  void overwriteCursorTest() {
    ups_cursor_t *c;
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    insertData(0, "1111111111");
    insertData(0, "2222222222");
    insertData(0, "33");
    insertData(0, "4444444444");
    insertData(0, "5555555555");

    checkData(c, UPS_CURSOR_FIRST,  0, "1111111111");
    checkData(c, UPS_CURSOR_NEXT,   0, "2222222222");
    checkData(c, UPS_CURSOR_NEXT,   0, "33");

    ::memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"3333333333333333333333333333333333333333333333333333";
    rec.size = (uint32_t)strlen((char *)rec.data)+1;
    REQUIRE(0 == ups_cursor_overwrite(c, &rec, 0));
    checkData(c, UPS_CURSOR_FIRST,  0, "1111111111");
    checkData(c, UPS_CURSOR_NEXT,   0, "2222222222");
    checkData(c, UPS_CURSOR_NEXT,   0,
        "3333333333333333333333333333333333333333333333333333");
    checkData(c, UPS_CURSOR_NEXT,   0, "4444444444");

    ::memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"44";
    rec.size = (uint32_t)strlen((char *)rec.data) + 1;
    REQUIRE(0 == ups_cursor_overwrite(c, &rec, 0));

    checkData(c, UPS_CURSOR_LAST,   0, "5555555555");
    checkData(c, UPS_CURSOR_PREVIOUS, 0, "44");
    checkData(c, UPS_CURSOR_PREVIOUS, 0,
        "3333333333333333333333333333333333333333333333333333");

    ups_cursor_close(c);
  }

  void overwriteMultipleCursorTest() {
    ups_cursor_t *c1, *c2, *c3;
    ups_key_t key;
    ups_record_t rec;
    ::memset(&key, 0, sizeof(key));

    REQUIRE(0 == ups_cursor_create(&c1, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c2, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c3, db, 0, 0));

    insertData(0, "1111111111");
    insertData(0, "2222222222");
    insertData(0, "33");
    insertData(0, "4444444444");
    insertData(0, "5555555555");

    checkData(c1, UPS_CURSOR_FIRST,  0, "1111111111");
    checkData(c1, UPS_CURSOR_NEXT,   0, "2222222222");
    checkData(c1, UPS_CURSOR_NEXT,   0, "33");
    checkData(c2, UPS_CURSOR_FIRST,  0, "1111111111");
    checkData(c3, UPS_CURSOR_FIRST,  0, "1111111111");
    checkData(c3, UPS_CURSOR_NEXT,   0, "2222222222");
    checkData(c3, UPS_CURSOR_NEXT,   0, "33");

    ::memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"3333333333333333333333333333333333333333333333333333";
    rec.size = (uint32_t)strlen((char *)rec.data) + 1;
    REQUIRE(0 == ups_cursor_overwrite(c1, &rec, 0));
    checkData(c1, 0,           0,
        "3333333333333333333333333333333333333333333333333333");
    checkData(c2, UPS_CURSOR_FIRST,  0, "1111111111");
    checkData(c1, UPS_CURSOR_FIRST,  0, "1111111111");
    checkData(c1, UPS_CURSOR_NEXT,   0, "2222222222");
    checkData(c1, UPS_CURSOR_NEXT,   0,
        "3333333333333333333333333333333333333333333333333333");
    checkData(c3, 0,           0,
        "3333333333333333333333333333333333333333333333333333");
    checkData(c1, UPS_CURSOR_NEXT,   0, "4444444444");
    checkData(c3, UPS_CURSOR_NEXT,   0, "4444444444");

    ::memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"44";
    rec.size = (uint32_t)strlen((char *)rec.data) + 1;
    REQUIRE(0 == ups_cursor_overwrite(c1, &rec, 0));
    checkData(c3, 0,           0, "44");
    checkData(c3, UPS_CURSOR_PREVIOUS, 0,
        "3333333333333333333333333333333333333333333333333333");
    checkData(c3, UPS_CURSOR_NEXT,   0, "44");
    checkData(c3, UPS_CURSOR_NEXT,   0, "5555555555");

    checkData(c1, UPS_CURSOR_LAST,   0, "5555555555");
    checkData(c1, UPS_CURSOR_PREVIOUS, 0, "44");
    checkData(c1, UPS_CURSOR_PREVIOUS, 0,
        "3333333333333333333333333333333333333333333333333333");
    checkData(c1, UPS_CURSOR_FIRST,  0, "1111111111");
    checkData(c2, UPS_CURSOR_FIRST,  0, "1111111111");

    ups_cursor_close(c1);
    ups_cursor_close(c2);
    ups_cursor_close(c3);
  }

  void invalidFlagsTest() {
    ups_cursor_t *c;

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_move(c, 0, 0,
            UPS_SKIP_DUPLICATES | UPS_ONLY_DUPLICATES));

    ups_cursor_close(c);
  }

  void overwriteTest() {
    ups_cursor_t *c1, *c2;
    ups_record_t rec;
    ups_key_t key;

    insertData(0, "111");
    insertData(0, "2222222222");
    insertData(0, "333");
    insertData(0, "4444444444");

    REQUIRE(0 == ups_cursor_create(&c1, db, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c2, db, 0, 0));

    checkData(c1, UPS_CURSOR_FIRST, 0, "111");
    checkData(c1, UPS_CURSOR_NEXT,  0, "2222222222");
    checkData(c2, UPS_CURSOR_FIRST, 0, "111");

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"1111111111111111111111111111111111111111";
    rec.size = (uint32_t)strlen((char *)rec.data)+1;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    checkData(c2, 0, 0, "1111111111111111111111111111111111111111");

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"00";
    rec.size = (uint32_t)strlen((char *)rec.data)+1;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    checkData(c2, 0, 0, "00");

    checkData(c1, UPS_CURSOR_PREVIOUS,  0, "00");
    checkData(c1, UPS_CURSOR_FIRST,   0, "00");

    ups_cursor_close(c1);
    ups_cursor_close(c2);
  }

  void eraseCursorTest() {
    ups_key_t key;
    ups_cursor_t *c;

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    insertData(0, "1111111111");
    insertData(0, "2222222222");
    insertData(0, "3333333333");
    insertData(0, "4444444444");
    insertData(0, "5555555555");

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(c, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(c, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(c, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(c, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(c, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(UPS_KEY_NOT_FOUND ==
            ups_cursor_find(c, &key, 0, 0));

    REQUIRE(0 == ups_cursor_close(c));
  }

  void insertLastTest() {
    ups_key_t key;
    ups_record_t rec;
    ups_cursor_t *c;
    const char *values[] = { "11111", "222222", "3333333", "44444444" };

    memset(&key, 0, sizeof(key));

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    for (int i = 0; i < 4; i++) {
      memset(&rec, 0, sizeof(rec));
      rec.data = (void *)values[i];
      rec.size = (uint32_t)strlen((char *)rec.data)+1;
      REQUIRE(0 ==
            ups_cursor_insert(c, &key, &rec,
                UPS_DUPLICATE_INSERT_LAST));
      memset(&rec, 0, sizeof(rec));
      REQUIRE(0 ==
            ups_cursor_move(c, 0, &rec, 0));
      REQUIRE(strlen(values[i]) == strlen((char *)rec.data));
      REQUIRE(0 == strcmp(values[i], (char *)rec.data));
      REQUIRE(i == ((LocalCursor *)c)->btree_cursor.duplicate_index());
    }

    checkData(c, UPS_CURSOR_FIRST, 0, values[0]);
    checkData(c, UPS_CURSOR_NEXT,  0, values[1]);
    checkData(c, UPS_CURSOR_NEXT,  0, values[2]);
    checkData(c, UPS_CURSOR_NEXT,  0, values[3]);
    checkData(c, UPS_CURSOR_NEXT,  UPS_KEY_NOT_FOUND, values[3]);

    REQUIRE(0 == ups_cursor_close(c));
  }

  void insertFirstTest() {
    ups_key_t key;
    ups_record_t rec;
    ups_cursor_t *c;
    const char *values[] = { "11111", "222222", "3333333", "44444444" };

    memset(&key, 0, sizeof(key));

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    for (int i = 0; i < 4; i++) {
      memset(&rec, 0, sizeof(rec));
      rec.data = (void *)values[i];
      rec.size = (uint32_t)strlen((char *)rec.data) + 1;
      REQUIRE(0 ==
            ups_cursor_insert(c, &key, &rec,
                UPS_DUPLICATE_INSERT_FIRST));
      memset(&rec, 0, sizeof(rec));
      REQUIRE(0 ==
            ups_cursor_move(c, 0, &rec, 0));
      REQUIRE(strlen((char *)rec.data) == strlen(values[i]));
      REQUIRE(0 == strcmp(values[i], (char *)rec.data));
      REQUIRE((uint32_t)0 ==
          ((LocalCursor *)c)->btree_cursor.duplicate_index());
    }

    checkData(c, UPS_CURSOR_FIRST, 0, values[3]);
    checkData(c, UPS_CURSOR_NEXT,  0, values[2]);
    checkData(c, UPS_CURSOR_NEXT,  0, values[1]);
    checkData(c, UPS_CURSOR_NEXT,  0, values[0]);
    checkData(c, UPS_CURSOR_NEXT,  UPS_KEY_NOT_FOUND, values[0]);

    REQUIRE(0 == ups_cursor_close(c));
  }

  void insertAfterTest() {
    ups_key_t key;
    ups_record_t rec;
    ups_cursor_t *c;
    const char *values[] = { "11111", "222222", "3333333", "44444444" };

    memset(&key, 0, sizeof(key));

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    for (int i = 0; i < 4; i++) {
      memset(&rec, 0, sizeof(rec));
      rec.data = (void *)values[i];
      rec.size = (uint32_t)strlen((char *)rec.data) + 1;
      REQUIRE(0 ==
            ups_cursor_insert(c, &key, &rec,
                UPS_DUPLICATE_INSERT_AFTER));
      memset(&rec, 0, sizeof(rec));
      REQUIRE(0 ==
            ups_cursor_move(c, 0, &rec, 0));
      REQUIRE(strlen((char *)rec.data) == strlen(values[i]));
      REQUIRE(0 == strcmp(values[i], (char *)rec.data));
      REQUIRE((i >= 1 ? 1 : 0) ==
            ((LocalCursor *)c)->btree_cursor.duplicate_index());
      REQUIRE(0 ==
            ups_cursor_move(c, 0, 0, UPS_CURSOR_FIRST));
    }

    checkData(c, UPS_CURSOR_FIRST,  0, values[0]);
    checkData(c, UPS_CURSOR_NEXT,   0, values[3]);
    checkData(c, UPS_CURSOR_NEXT,   0, values[2]);
    checkData(c, UPS_CURSOR_NEXT,   0, values[1]);
    checkData(c, UPS_CURSOR_NEXT,   UPS_KEY_NOT_FOUND, values[0]);

    REQUIRE(0 == ups_cursor_close(c));
  }

  void insertBeforeTest() {
    ups_key_t key = {0};
    ups_cursor_t *c;
    const char *values[] = { "11111", "222222", "3333333", "44444444" };

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    for (int i = 0; i < 4; i++) {
      ups_record_t rec = ups_make_record((void *)values[i],
                            (uint32_t)::strlen(values[i]) + 1);
      REQUIRE(0 == ups_cursor_insert(c, &key, &rec,
                              UPS_DUPLICATE_INSERT_BEFORE));
      memset(&rec, 0, sizeof(rec));
      REQUIRE(0 == ups_cursor_move(c, 0, &rec, 0));
      REQUIRE(::strlen((char *)rec.data) == ::strlen(values[i]));
      REQUIRE(0 == ::strcmp(values[i], (char *)rec.data));
      int di = ((LocalCursor *)c)->btree_cursor.duplicate_index();
      if (i <= 1)
        REQUIRE(di == 0);
      else
        REQUIRE(di == i -1);
      REQUIRE(0 == ups_cursor_move(c, 0, 0, UPS_CURSOR_LAST));
    }

    checkData(c, UPS_CURSOR_FIRST,  0, values[1]);
    checkData(c, UPS_CURSOR_NEXT,   0, values[2]);
    checkData(c, UPS_CURSOR_NEXT,   0, values[3]);
    checkData(c, UPS_CURSOR_NEXT,   0, values[0]);
    checkData(c, UPS_CURSOR_NEXT,   UPS_KEY_NOT_FOUND, values[0]);

    REQUIRE(0 == ups_cursor_close(c));
  }

  void overwriteVariousSizesTest() {
    ups_key_t key;
    ups_record_t rec;
    ups_cursor_t *c;
    uint32_t sizes[] = { 0, 6, 8, 10 };
    const char *values[] = { 0, "55555", "8888888", "999999999" };
    const char *newvalues[4];

    memset(&key, 0, sizeof(key));

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    for (int s = 0; s < 5; s++) {
      for (int i = s, j = 0; i < s + 4; i++, j++) {
        memset(&rec, 0, sizeof(rec));
        rec.size = sizes[i % 4];
        if (sizes[i % 4]) {
          rec.data = (void *)values[i % 4];
          newvalues[j] = values[i % 4];
        }
        else {
          rec.data = 0;
          newvalues[j] = 0;
        }

        if (s == 0) {
          /* first round: insert the duplicates */
          REQUIRE(0 == ups_cursor_insert(c, &key, &rec,
                  UPS_DUPLICATE_INSERT_LAST));
        }
        else {
          /* other rounds: just overwrite them */
          REQUIRE(0 == ups_cursor_overwrite(c, &rec, 0));
          if (i != (s + 4) - 1)
            REQUIRE(0 == ups_cursor_move(c, 0, 0, UPS_CURSOR_NEXT));
        }
      }

      checkData(c, UPS_CURSOR_FIRST, 0, newvalues[0]);
      checkData(c, UPS_CURSOR_NEXT,  0, newvalues[1]);
      checkData(c, UPS_CURSOR_NEXT,  0, newvalues[2]);
      checkData(c, UPS_CURSOR_NEXT,  0, newvalues[3]);
      checkData(c, UPS_CURSOR_NEXT,  UPS_KEY_NOT_FOUND, newvalues[1]);

      /* move to first element */
      checkData(c, UPS_CURSOR_FIRST, 0, newvalues[0]);
    }

    REQUIRE(0 == ups_cursor_close(c));
  }

  void getDuplicateCountTest() {
    uint32_t count;
    ups_cursor_t *c;

    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_get_duplicate_count(0, &count, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_get_duplicate_count(c, 0, 0));
    REQUIRE(UPS_CURSOR_IS_NIL ==
        ups_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((uint32_t)0 == count);

    insertData(0, "1111111111");
    checkData(c, UPS_CURSOR_NEXT,   0, "1111111111");
    REQUIRE(0 ==
        ups_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((uint32_t)1 == count);

    insertData(0, "2222222222");
    checkData(c, UPS_CURSOR_NEXT,   0, "2222222222");
    REQUIRE(0 ==
        ups_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((uint32_t)2 == count);

    insertData(0, "3333333333");
    checkData(c, UPS_CURSOR_NEXT,   0, "3333333333");
    ((LocalCursor *)c)->btree_cursor.uncouple_from_page(context.get());
    REQUIRE(0 == ups_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((uint32_t)3 == count);

    REQUIRE(0 == ups_cursor_erase(c, 0));
    REQUIRE(UPS_CURSOR_IS_NIL ==
        ups_cursor_get_duplicate_count(c, &count, 0));
    checkData(c, UPS_CURSOR_FIRST,  0, "1111111111");
    REQUIRE(0 ==
        ups_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((uint32_t)2 == count);

    REQUIRE(0 == ups_cursor_close(c));

    if (!is_in_memory()) {
      /* reopen the database */
      teardown();
      require_open(m_flags);
      REQUIRE(ISSET(ldb()->flags(), UPS_ENABLE_DUPLICATE_KEYS));

      REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

      checkData(c, UPS_CURSOR_NEXT,   0, "1111111111");
      REQUIRE(0 ==
          ups_cursor_get_duplicate_count(c, &count, 0));
      REQUIRE((uint32_t)2 == count);

      REQUIRE(0 == ups_cursor_close(c));
    }
  }

  void insertManyManyTest() {
    ups_key_t key;
    ups_record_t rec;
    ups_cursor_t *c;
    ups_parameter_t params[2] = {
      { UPS_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };

    teardown();
    require_create(m_flags, params, UPS_ENABLE_DUPLICATE_KEYS, nullptr);

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    for (int i = 0; i < 1000; i++) {
      memset(&rec, 0, sizeof(rec));
      rec.size = sizeof(i);
      rec.data = &i;

      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));
    }

    for (int i = 0; i < 1000; i++) {
      memset(&rec, 0, sizeof(rec));

      REQUIRE(0 == ups_cursor_move(c, &key, &rec, UPS_CURSOR_NEXT));
      REQUIRE((uint32_t)4 == rec.size);
      REQUIRE(i == *(int *)rec.data);
    }

    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_move(c, 0, 0, UPS_CURSOR_NEXT));
    REQUIRE(0 == ups_cursor_close(c));
  }

  void cloneTest() {
    ups_cursor_t *c1, *c2;
    int value;
    ups_key_t key = {0};
    ups_record_t rec = ups_make_record(&value, sizeof(value));

    value = 1;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    value = 2;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    value = 3;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_DUPLICATE));

    REQUIRE(0 == ups_cursor_create(&c1, db, 0, 0));

    REQUIRE(0 == ups_cursor_move(c1, &key, &rec, UPS_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);
    REQUIRE(0 == ups_cursor_move(c1, &key, &rec, UPS_CURSOR_NEXT));
    REQUIRE(2 == *(int *)rec.data);
    REQUIRE(0 == ups_cursor_clone(c1, &c2));

    REQUIRE(0 == ups_cursor_move(c2, &key, &rec, UPS_CURSOR_NEXT));
    REQUIRE(3 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_erase(c1, 0));
    REQUIRE(((LocalCursor *)c1)->is_nil(LocalCursor::kBtree));
    REQUIRE(!((LocalCursor *)c2)->is_nil(LocalCursor::kBtree));

    REQUIRE(0 == ups_cursor_move(c2, &key, &rec, 0));
    REQUIRE(3 == *(int *)rec.data);

    REQUIRE(0 == ups_cursor_close(c1));
    REQUIRE(0 == ups_cursor_close(c2));
  }
};

TEST_CASE("DuplicateFixture/invalidFlagsTest", "")
{
  DuplicateFixture f;
  f.invalidFlagsTest();
}

TEST_CASE("DuplicateFixture/insertDuplicatesTest", "")
{
  DuplicateFixture f;
  f.insertDuplicatesTest();
}

TEST_CASE("DuplicateFixture/insertDuplicatesFirstTest", "")
{
  DuplicateFixture f;
  f.insertDuplicatesFirstTest();
}

TEST_CASE("DuplicateFixture/overwriteDuplicatesTest", "")
{
  DuplicateFixture f;
  f.overwriteDuplicatesTest();
}

TEST_CASE("DuplicateFixture/overwriteVariousDuplicatesTest", "")
{
  DuplicateFixture f;
  f.overwriteVariousDuplicatesTest();
}

TEST_CASE("DuplicateFixture/insertMoveForwardTest", "")
{
  DuplicateFixture f;
  f.insertMoveForwardTest();
}

TEST_CASE("DuplicateFixture/insertMoveBackwardTest", "")
{
  DuplicateFixture f;
  f.insertMoveBackwardTest();
}

TEST_CASE("DuplicateFixture/insertEraseTest", "")
{
  DuplicateFixture f;
  f.insertEraseTest();
}

TEST_CASE("DuplicateFixture/insertTest", "")
{
  DuplicateFixture f;
  f.insertTest();
}

TEST_CASE("DuplicateFixture/insertSkipDuplicatesTest", "")
{
  DuplicateFixture f;
  f.insertSkipDuplicatesTest();
}

TEST_CASE("DuplicateFixture/insertOnlyDuplicatesTest", "")
{
  DuplicateFixture f;
  f.insertOnlyDuplicatesTest();
}

TEST_CASE("DuplicateFixture/insertOnlyDuplicatesTest2", "")
{
  DuplicateFixture f;
  f.insertOnlyDuplicatesTest2();
}

TEST_CASE("DuplicateFixture/coupleUncoupleTest", "")
{
  DuplicateFixture f;
  f.coupleUncoupleTest();
}

TEST_CASE("DuplicateFixture/moveToLastDuplicateTest", "")
{
  DuplicateFixture f;
  f.moveToLastDuplicateTest();
}

/*
 * insert 2 dupes, create 2 cursors (both on the first dupe).
 * delete the first cursor, make sure that both cursors are
 * NILled and the second dupe is still available
 */
TEST_CASE("DuplicateFixture/eraseDuplicateTest", "")
{
  DuplicateFixture f;
  f.eraseDuplicateTest();
}

/*
 * same as above, but uncouples the cursor before the first cursor
 * is deleted
 */
TEST_CASE("DuplicateFixture/eraseDuplicateUncoupledTest", "")
{
  DuplicateFixture f;
  f.eraseDuplicateUncoupledTest();
}

/*
 * insert 2 dupes, create 2 cursors (both on the second dupe).
 * delete the first cursor, make sure that both cursors are
 * NILled and the first dupe is still available
 */
TEST_CASE("DuplicateFixture/eraseSecondDuplicateTest", "")
{
  DuplicateFixture f;
  f.eraseSecondDuplicateTest();
}

/*
 * same as above, but uncouples the cursor before the second cursor
 * is deleted
 */
TEST_CASE("DuplicateFixture/eraseSecondDuplicateUncoupledTest", "")
{
  DuplicateFixture f;
  f.eraseSecondDuplicateUncoupledTest();
}

/*
 * insert 2 dupes, create 2 cursors (one on the first, the other on the
 * second dupe). delete the first cursor, make sure that it's NILled
 * and the other cursor is still valid.
 */
TEST_CASE("DuplicateFixture/eraseOtherDuplicateTest", "")
{
  DuplicateFixture f;
  f.eraseOtherDuplicateTest();
}

/*
 * same as above, but uncouples the cursor before the second cursor
 * is deleted
 */
TEST_CASE("DuplicateFixture/eraseOtherDuplicateUncoupledTest", "")
{
  DuplicateFixture f;
  f.eraseOtherDuplicateUncoupledTest();
}

/*
 * inserts 3 dupes, creates 2 cursors on the middle item; delete the
 * first cursor, make sure that the second is NILled and that the first
 * and last item still exists
 */
TEST_CASE("DuplicateFixture/eraseMiddleDuplicateTest", "")
{
  DuplicateFixture f;
  f.eraseMiddleDuplicateTest();
}

/*
 * inserts a few TINY dupes, then erases them all but the last element
 */
TEST_CASE("DuplicateFixture/eraseTinyDuplicatesTest", "")
{
  DuplicateFixture f;
  f.eraseTinyDuplicatesTest();
}

/*
 * inserts a few duplicates, reopens the database; continues inserting
 */
TEST_CASE("DuplicateFixture/reopenTest", "")
{
  DuplicateFixture f;
  f.reopenTest();
}

/*
 * test ups_cursor_move(... UPS_CURSOR_PREVIOUS)
 */
TEST_CASE("DuplicateFixture/moveToPreviousDuplicateTest", "")
{
  DuplicateFixture f;
  f.moveToPreviousDuplicateTest();
}

/*
 * overwrite duplicates using ups_db_insert(... UPS_OVERWRITE)
 */
TEST_CASE("DuplicateFixture/overwriteTest", "")
{
  DuplicateFixture f;
  f.overwriteTest();
}

/*
 * overwrite duplicates using ups_cursor_insert(... UPS_OVERWRITE)
 */
TEST_CASE("DuplicateFixture/overwriteCursorTest", "")
{
  DuplicateFixture f;
  f.overwriteCursorTest();
}

/*
 * same as overwriteCursorTest, but uses multiple cursors and makes
 * sure that their positions are not modified
 */
TEST_CASE("DuplicateFixture/overwriteMultipleCursorTest", "")
{
  DuplicateFixture f;
  f.overwriteMultipleCursorTest();
}

/*
 * insert a few duplicate items, then delete them all with a cursor
 */
TEST_CASE("DuplicateFixture/eraseCursorTest", "")
{
  DuplicateFixture f;
  f.eraseCursorTest();
}

/*
 * tests UPS_DUPLICATE_INSERT_LAST and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture/insertLastTest", "")
{
  DuplicateFixture f;
  f.insertLastTest();
}

/*
 * tests UPS_DUPLICATE_INSERT_FIRST and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture/insertFirstTest", "")
{
  DuplicateFixture f;
  f.insertFirstTest();
}

/*
 * tests UPS_DUPLICATE_INSERT_AFTER and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture/insertAfterTest", "")
{
  DuplicateFixture f;
  f.insertAfterTest();
}

/*
 * tests UPS_DUPLICATE_INSERT_BEFORE and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture/insertBeforeTest", "")
{
  DuplicateFixture f;
  f.insertBeforeTest();
}

/*
 * overwrite NULL-, TINY- and SMALL-duplicates with other
 * NULL-, TINY- and SMALL-duplicates
 */
TEST_CASE("DuplicateFixture/overwriteVariousSizesTest", "")
{
  DuplicateFixture f;
  f.overwriteVariousSizesTest();
}

/*
 * tests get_cuplicate_count
 */
TEST_CASE("DuplicateFixture/getDuplicateCountTest", "")
{
  DuplicateFixture f;
  f.getDuplicateCountTest();
}

/*
 * insert a lot of duplicates (the duplicate table will grow)
 */
TEST_CASE("DuplicateFixture/insertManyManyTest", "")
{
  DuplicateFixture f;
  f.insertManyManyTest();
}

/*
 * insert several duplicates; then set a cursor to the 2nd duplicate.
 * clone the cursor, move it to the next element. then erase the
 * first cursor.
 */
TEST_CASE("DuplicateFixture/cloneTest", "")
{
  DuplicateFixture f;
  f.cloneTest();
}


TEST_CASE("DuplicateFixture/inmem/invalidFlagsTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.invalidFlagsTest();
}

TEST_CASE("DuplicateFixture/inmem/insertDuplicatesTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertDuplicatesTest();
}

TEST_CASE("DuplicateFixture/inmem/overwriteDuplicatesTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.overwriteDuplicatesTest();
}

TEST_CASE("DuplicateFixture/inmem/overwriteVariousDuplicatesTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.overwriteVariousDuplicatesTest();
}

TEST_CASE("DuplicateFixture/inmem/insertMoveForwardTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertMoveForwardTest();
}

TEST_CASE("DuplicateFixture/inmem/insertMoveBackwardTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertMoveBackwardTest();
}

TEST_CASE("DuplicateFixture/inmem/insertEraseTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertEraseTest();
}

TEST_CASE("DuplicateFixture/inmem/insertTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertTest();
}

TEST_CASE("DuplicateFixture/inmem/insertSkipDuplicatesTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertSkipDuplicatesTest();
}

TEST_CASE("DuplicateFixture/inmem/insertOnlyDuplicatesTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertOnlyDuplicatesTest();
}

TEST_CASE("DuplicateFixture/inmem/insertOnlyDuplicatesTest2", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertOnlyDuplicatesTest2();
}

TEST_CASE("DuplicateFixture/inmem/coupleUncoupleTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.coupleUncoupleTest();
}

TEST_CASE("DuplicateFixture/inmem/moveToLastDuplicateTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.moveToLastDuplicateTest();
}

/*
 * insert 2 dupes, create 2 cursors (both on the first dupe).
 * delete the first cursor, make sure that both cursors are
 * NILled and the second dupe is still available
 */
TEST_CASE("DuplicateFixture/inmem/eraseDuplicateTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.eraseDuplicateTest();
}

/*
 * same as above, but uncouples the cursor before the first cursor
 * is deleted
 */
TEST_CASE("DuplicateFixture/inmem/eraseDuplicateUncoupledTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.eraseDuplicateUncoupledTest();
}

/*
 * insert 2 dupes, create 2 cursors (both on the second dupe).
 * delete the first cursor, make sure that both cursors are
 * NILled and the first dupe is still available
 */
TEST_CASE("DuplicateFixture/inmem/eraseSecondDuplicateTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.eraseSecondDuplicateTest();
}

/*
 * same as above, but uncouples the cursor before the second cursor
 * is deleted
 */
TEST_CASE("DuplicateFixture/inmem/eraseSecondDuplicateUncoupledTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.eraseSecondDuplicateUncoupledTest();
}

/*
 * insert 2 dupes, create 2 cursors (one on the first, the other on the
 * second dupe). delete the first cursor, make sure that it's NILled
 * and the other cursor is still valid.
 */
TEST_CASE("DuplicateFixture/inmem/eraseOtherDuplicateTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.eraseOtherDuplicateTest();
}

/*
 * same as above, but uncouples the cursor before the second cursor
 * is deleted
 */
TEST_CASE("DuplicateFixture/inmem/eraseOtherDuplicateUncoupledTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.eraseOtherDuplicateUncoupledTest();
}

/*
 * inserts 3 dupes, creates 2 cursors on the middle item; delete the
 * first cursor, make sure that the second is NILled and that the first
 * and last item still exists
 */
TEST_CASE("DuplicateFixture/inmem/eraseMiddleDuplicateTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.eraseMiddleDuplicateTest();
}

/*
 * inserts a few TINY dupes, then erases them all but the last element
 */
TEST_CASE("DuplicateFixture/inmem/eraseTinyDuplicatesTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.eraseTinyDuplicatesTest();
}

/*
 * inserts a few duplicates, reopens the database; continues inserting
 */
TEST_CASE("DuplicateFixture/inmem/reopenTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.reopenTest();
}

/*
 * test ups_cursor_move(... UPS_CURSOR_PREVIOUS)
 */
TEST_CASE("DuplicateFixture/inmem/moveToPreviousDuplicateTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.moveToPreviousDuplicateTest();
}

/*
 * overwrite duplicates using ups_db_insert(... UPS_OVERWRITE)
 */
TEST_CASE("DuplicateFixture/inmem/overwriteTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.overwriteTest();
}

/*
 * overwrite duplicates using ups_cursor_insert(... UPS_OVERWRITE)
 */
TEST_CASE("DuplicateFixture/inmem/overwriteCursorTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.overwriteCursorTest();
}

/*
 * same as overwriteCursorTest, but uses multiple cursors and makes
 * sure that their positions are not modified
 */
TEST_CASE("DuplicateFixture/inmem/overwriteMultipleCursorTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.overwriteMultipleCursorTest();
}

/*
 * insert a few duplicate items, then delete them all with a cursor
 */
TEST_CASE("DuplicateFixture/inmem/eraseCursorTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.eraseCursorTest();
}

/*
 * tests UPS_DUPLICATE_INSERT_LAST and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture/inmem/insertLastTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertLastTest();
}

/*
 * tests UPS_DUPLICATE_INSERT_FIRST and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture/inmem/insertFirstTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertFirstTest();
}

/*
 * tests UPS_DUPLICATE_INSERT_AFTER and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture/inmem/insertAfterTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertAfterTest();
}

/*
 * tests UPS_DUPLICATE_INSERT_BEFORE and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture/inmem/insertBeforeTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertBeforeTest();
}

/*
 * overwrite NULL-, TINY- and SMALL-duplicates with other
 * NULL-, TINY- and SMALL-duplicates
 */
TEST_CASE("DuplicateFixture/inmem/overwriteVariousSizesTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.overwriteVariousSizesTest();
}

/*
 * tests get_cuplicate_count
 */
TEST_CASE("DuplicateFixture/inmem/getDuplicateCountTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.getDuplicateCountTest();
}

/*
 * insert a lot of duplicates to grow the duplicate table
 */
TEST_CASE("DuplicateFixture/inmem/insertManyManyTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.insertManyManyTest();
}

/*
 * insert several duplicates; then set a cursor to the 2nd duplicate.
 * clone the cursor, move it to the next element. then erase the
 * first cursor.
 */
TEST_CASE("DuplicateFixture/inmem/cloneTest", "")
{
  DuplicateFixture f(UPS_IN_MEMORY);
  f.cloneTest();
}

} // namespace upscaledb
