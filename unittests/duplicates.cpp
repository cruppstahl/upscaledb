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

#include <vector>
#include <string>

#include "3rdparty/catch/catch.hpp"

#include "globals.h"
#include "os.hpp"

#include "../src/db.h"
#include "../src/blob_manager.h"
#include "../src/page_manager.h"
#include "../src/btree.h"
#include "../src/endianswap.h"
#include "../src/cursor.h"
#include "../src/env.h"
#include "../src/btree_cursor.h"

namespace hamsterdb {

struct DuplicateFixture {
  ham_u32_t m_flags;
  ham_db_t *m_db;
  ham_env_t *m_env;
  std::vector<std::string> m_data;

  DuplicateFixture(ham_u32_t flags = 0)
    : m_flags(flags) {
    (void)os::unlink(Globals::opath(".test"));

    REQUIRE(0 == ham_env_create(&m_env, Globals::opath(".test"),
          m_flags, 0664, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1,
          HAM_ENABLE_DUPLICATES, 0));

    m_data.resize(0);
  }

  ~DuplicateFixture() {
    teardown();
  }

  void teardown() {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void insertDuplicatesTest() {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_record_t rec2 = {};
    char data[16];

    for (int i = 0; i < 10; i++) {
      rec.data = data;
      rec.size = sizeof(data);
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
          ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
    }

    ::memset(&data, 0x15, sizeof(data));
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec2, 0));
    REQUIRE((ham_size_t)sizeof(data) == rec2.size);
    REQUIRE(0 == ::memcmp(data, rec2.data, sizeof(data)));
  }

  void overwriteDuplicatesTest()
  {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_record_t rec2 = {};
    ham_cursor_t *c;
    ham_size_t count;
    char data[16];

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    for (int i = 0; i < 5; i++) {
      rec.data = data;
      rec.size = sizeof(data);
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
          ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
    }

    rec.data = data;
    rec.size = sizeof(data);
    ::memset(&data, 0x99, sizeof(data));
    REQUIRE(0 ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));

    REQUIRE(0 == ham_cursor_move(c, &key, &rec2, HAM_CURSOR_FIRST));
    REQUIRE((ham_size_t)sizeof(data) == rec2.size);
    REQUIRE(0 == ::memcmp(data, rec2.data, sizeof(data)));

    for (int i = 1; i < 5; i++) {
      ::memset(&data, i+0x15, sizeof(data));
      REQUIRE(0 ==
          ham_cursor_move(c, &key, &rec2, HAM_CURSOR_NEXT));
      REQUIRE((ham_size_t)sizeof(data) == rec2.size);
      REQUIRE(0 == ::memcmp(data, rec2.data, sizeof(data)));
    }

    REQUIRE(0 ==
        ham_cursor_move(c, 0, 0, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((ham_size_t)5 == count);

    REQUIRE(0 == ham_cursor_close(c));
  }

  void overwriteVariousDuplicatesTest() {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_record_t rec2 = {};
#define M  10
    unsigned sizes[M] = { 0, 1, 2, 3, 4, 5, 936, 5, 100, 50 };
    char *data = 0;
    ham_cursor_t *cursor;
    ham_size_t count;

    for (unsigned i = 0; i < M; i++) {
      data = 0;
      if (sizes[i]) {
        data = (char *)malloc(sizes[i]);
        ::memset(data, i + 0x15, sizes[i]);
      }
      rec.data = sizes[i] ? data : 0;
      rec.size = sizes[i];
      REQUIRE(0 ==
          ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
      REQUIRE(sizes[i] == rec.size);
      if (sizes[i]) {
        REQUIRE(0 == ::memcmp(data, rec.data, sizes[i]));
        free(data);
      }
    }

    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec2, 0));
    REQUIRE((ham_size_t)0 == rec2.size);

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    for (unsigned i = 0; i < M; i++) {
      if (sizes[i]) {
        data = (char *)malloc(sizes[i]);
        ::memset(data, i+0x15, sizes[i]);
      }
      rec.data = sizes[i] ? data : 0;
      rec.size = sizes[i];
      REQUIRE(0 ==
          ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
      REQUIRE(sizes[i] == rec.size);
      if (sizes[i]) {
        REQUIRE(0 == ::memcmp(data, rec.data, sizes[i]));
        free(data);
      }
    }

    REQUIRE(0 ==
        ham_cursor_get_duplicate_count(cursor, &count, 0));
    REQUIRE((ham_size_t)M == count);

    REQUIRE(0 == ham_cursor_close(cursor));
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    for (int i = M - 1; i >= 0; i--) {
      if (sizes[i]) {
        data = (char *)malloc(sizes[i]);
        ::memset(data, i+0x15, sizes[i]);
      }
      rec.data = sizes[i] ? data : 0;
      rec.size = sizes[i];
      REQUIRE(0 ==
          ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_PREVIOUS));
      REQUIRE(sizes[i] == rec.size);
      if (sizes[i]) {
        REQUIRE(0 == ::memcmp(data, rec.data, sizes[i]));
        free(data);
      }
    }

    REQUIRE(0 ==
        ham_cursor_get_duplicate_count(cursor, &count, 0));
    REQUIRE((ham_size_t)M == count);

    REQUIRE(0 == ham_cursor_close(cursor));

    data = (char *)malloc(16);
    ::memset(data, 0x99, 16);
    rec.data = data;
    rec.size = 16;
    REQUIRE(0 ==
        ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));

    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec2, 0));
    REQUIRE((ham_size_t)16 == rec2.size);
    REQUIRE(0 == ::memcmp(data, rec2.data, 16));
    free(data);
  }

  void insertMoveForwardTest() {
    ham_key_t key;
    ham_record_t rec;
    ham_cursor_t *cursor;
    char data[16];

    for (int i = 0; i < 5; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      rec.data = data;
      rec.size = sizeof(data);
      ::memset(&data, i+0x15, sizeof(data));
      REQUIRE(0 ==
          ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
    }

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    for (int i = 0; i < 5; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
          ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
      REQUIRE((ham_size_t)sizeof(data) == rec.size);
      REQUIRE(0 == ::memcmp(data, rec.data, sizeof(data)));
    }

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));

    REQUIRE(0 == ham_cursor_close(cursor));
  }

  void insertMoveBackwardTest() {
    ham_key_t key;
    ham_record_t rec;
    ham_cursor_t *cursor;
    char data[16];

    for (int i = 0; i < 5; i++) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      rec.data = data;
      rec.size = sizeof(data);
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
          ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
    }

    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    for (int i = 4; i >= 0; i--) {
      ::memset(&key, 0, sizeof(key));
      ::memset(&rec, 0, sizeof(rec));
      ::memset(&data, i + 0x15, sizeof(data));
      REQUIRE(0 ==
          ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_PREVIOUS));
      REQUIRE((ham_size_t)sizeof(data) == rec.size);
      REQUIRE(0 == ::memcmp(data, rec.data, sizeof(data)));
    }

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_PREVIOUS));

    REQUIRE(0 == ham_cursor_close(cursor));
  }

  void insertEraseTest() {
    ham_key_t key;
    ham_record_t rec;
    char data[16];
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    ::memset(&data, 0x13, sizeof(data));
    rec.data = data;
    rec.size = sizeof(data);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    ::memset(&data, 0x14, sizeof(data));
    rec.data = data;
    rec.size = sizeof(data);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    ::memset(&data, 0x15, sizeof(data));
    rec.data = data;
    rec.size = sizeof(data);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    ::memset(&rec, 0, sizeof(rec));
    ::memset(&data, 0x13, sizeof(data));
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE((ham_size_t)sizeof(data) == rec.size);
    REQUIRE(0 == ::memcmp(data, rec.data, sizeof(data)));

    REQUIRE(0 == ham_db_erase(m_db, 0, &key, 0));
    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_db_find(m_db, 0, &key, &rec, 0));
  }

  void insert(ham_key_t *key, ham_record_t *rec) {
    REQUIRE(0 == ham_db_insert(m_db, 0, key, rec, HAM_DUPLICATE));
  }

  void find(ham_key_t *key, ham_record_t *rec) {
    ham_record_t record = {};

    REQUIRE(0 == ham_db_find(m_db, 0, key, &record, 0));
    REQUIRE(rec->size == record.size);
    REQUIRE(0 == ::memcmp(rec->data, record.data, rec->size));

    rec->_rid = record._rid;
  }

  void erase(ham_key_t *key) {
    REQUIRE(0 == ham_db_erase(m_db, 0, key, 0));
  }

  void insertData(const char *k, const char *data) {
    ham_key_t key = {};
    ham_record_t rec = {};
    rec.data = (void *)data;
    rec.size = (ham_size_t)::strlen(data)+1;
    key.data = (void *)k;
    key.size = (ham_u16_t)(k ? ::strlen(k)+1 : 0);

    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
  }

  void checkData(ham_cursor_t *cursor, ham_u32_t flags,
      ham_status_t expected, const char *data) {
    ham_key_t key = {};
    ham_record_t rec = {};

    REQUIRE(expected ==
        ham_cursor_move(cursor, &key, &rec, flags));

    if (expected == 0) {
      if (data) {
        REQUIRE(rec.size == (ham_size_t)::strlen(data) + 1);
        REQUIRE(0 == ::memcmp(rec.data, data, rec.size));
      }
      else {
        REQUIRE(rec.size == (ham_size_t)0);
        REQUIRE((void *)0 == rec.data);
      }
    }
  }

  void insertTest() {
    ham_cursor_t *c;
    ham_size_t count;

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

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

    checkData(c, HAM_CURSOR_NEXT,   0, "1111111111");
    checkData(c, HAM_CURSOR_NEXT,   0, "2222222222");
    checkData(c, HAM_CURSOR_NEXT,   0, "3333333333");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "2222222222");
    checkData(c, HAM_CURSOR_NEXT,   0, "3333333333");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "2222222222");
    checkData(c, HAM_CURSOR_NEXT,   0, "3333333333");
    checkData(c, HAM_CURSOR_NEXT,   0, "4444444444");
    checkData(c, HAM_CURSOR_NEXT,   0, "5555555555");
    checkData(c, HAM_CURSOR_NEXT,   0, "6666666666");
    checkData(c, HAM_CURSOR_NEXT,   0, "7777777777");
    checkData(c, HAM_CURSOR_NEXT,   0, "8888888888");
    checkData(c, HAM_CURSOR_NEXT,   0, "9999999999");
    checkData(c, HAM_CURSOR_NEXT,   0, "0000000000");
    checkData(c, HAM_CURSOR_NEXT,   HAM_KEY_NOT_FOUND, "0000000000");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "9999999999");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "8888888888");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "7777777777");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "6666666666");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "5555555555");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "4444444444");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "3333333333");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "2222222222");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "1111111111");
    checkData(c, HAM_CURSOR_PREVIOUS, HAM_KEY_NOT_FOUND, "0000000000");
    checkData(c, HAM_CURSOR_NEXT,   0, "2222222222");
    checkData(c, HAM_CURSOR_NEXT,   0, "3333333333");

    REQUIRE(0 ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((ham_size_t)10 == count);

    ham_cursor_close(c);
  }

  void insertSkipDuplicatesTest() {
    ham_cursor_t *c;

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

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

    checkData(c, HAM_CURSOR_NEXT,   0, "aaaaaaaaaa");
    checkData(c, HAM_CURSOR_NEXT,   0, "1111111111");
    checkData(c, HAM_CURSOR_NEXT,   0, "2222222222");
    checkData(c, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
    checkData(c, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES, 0, "cccccccccc");
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "1111111111");
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "aaaaaaaaaa");

    ham_cursor_close(c);
  }

  void insertOnlyDuplicatesTest() {
    ham_cursor_t *c;

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    insertData("000", "aaaaaaaaaa");
    insertData("111", "8888888888");
    insertData("111", "9999999999");
    insertData("111", "0000000000");
    insertData("222", "bbbbbbbbbb");

    checkData(c, HAM_CURSOR_FIRST,  0, "aaaaaaaaaa");
    checkData(c, HAM_CURSOR_NEXT,   0, "8888888888");
    checkData(c, HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES, 0, "9999999999");
    checkData(c, HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES, 0, "0000000000");
    checkData(c, HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES, HAM_KEY_NOT_FOUND, 0);

    checkData(c, HAM_CURSOR_FIRST,  0, "aaaaaaaaaa");
    checkData(c, HAM_CURSOR_NEXT,   0, "8888888888");
    checkData(c, HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES, 0, "9999999999");
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_ONLY_DUPLICATES, 0, "8888888888");
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_ONLY_DUPLICATES,
            HAM_KEY_NOT_FOUND, 0);

    checkData(c, HAM_CURSOR_FIRST,  0, "aaaaaaaaaa");
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_ONLY_DUPLICATES,
            HAM_KEY_NOT_FOUND, 0);

    ham_cursor_close(c);
  }

  void insertOnlyDuplicatesTest2() {
    ham_cursor_t *c;

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    insertData("1", "1");
    insertData("1", "2");
    insertData("1", "3");
    insertData("1", "4");

    const char *exp[] = { "1", "2", "3", "4" };

    ham_key_t key = {};
    key.data = (void *)"1";
    key.size = 2;
    ham_record_t rec = {};

    REQUIRE(0 == ham_cursor_find(c, &key, 0, 0));
    for (int i = 0; i < 3; i++) {
      REQUIRE(0 == ham_cursor_move(c, 0, &rec, 0));
      REQUIRE(0 == strcmp(exp[i], (char *)rec.data));
      REQUIRE(0 ==
          ham_cursor_move(c, &key, &rec,
              HAM_CURSOR_NEXT | HAM_ONLY_DUPLICATES));
    }

    checkData(c, HAM_CURSOR_NEXT | HAM_ONLY_DUPLICATES,
        HAM_KEY_NOT_FOUND, 0);

    ham_cursor_close(c);
  }

  void coupleUncoupleTest() {
    ham_cursor_t *c;
    Page *page;

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

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

    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    REQUIRE(0 == ((Environment *)m_env)->get_page_manager()->fetch_page(&page,
                (LocalDatabase *)m_db, be->get_root_address()));
    REQUIRE(page);

    REQUIRE(0 == BtreeCursor::uncouple_all_cursors(page));
    checkData(c, HAM_CURSOR_NEXT,   0, "aaaaaaaaaa");
    REQUIRE(0 == BtreeCursor::uncouple_all_cursors(page));
    REQUIRE(0 == BtreeCursor::uncouple_all_cursors(page));
    checkData(c, HAM_CURSOR_NEXT,   0, "1111111111");
    REQUIRE(0 == BtreeCursor::uncouple_all_cursors(page));
    checkData(c, HAM_CURSOR_NEXT,   0, "2222222222");
    REQUIRE(0 == BtreeCursor::uncouple_all_cursors(page));
    checkData(c, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
    REQUIRE(0 == BtreeCursor::uncouple_all_cursors(page));
    checkData(c, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES, 0, "cccccccccc");
    REQUIRE(0 == BtreeCursor::uncouple_all_cursors(page));
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
    REQUIRE(0 == BtreeCursor::uncouple_all_cursors(page));
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "1111111111");
    REQUIRE(0 == BtreeCursor::uncouple_all_cursors(page));
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "aaaaaaaaaa");

    ham_cursor_close(c);
  }

  void eraseOtherDuplicateUncoupledTest() {
    ham_cursor_t *c1, *c2;
    ham_key_t key = {};
    ham_record_t rec = {};
    int value = 1;

    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    REQUIRE(0 == ham_cursor_create(&c1, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c2, m_db, 0, 0));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 ==
        ((Cursor *)c2)->get_btree_cursor()->uncouple_from_page());
    REQUIRE(0 == ham_cursor_erase(c1, 0));
    REQUIRE(((Cursor *)c1)->is_nil(Cursor::kBtree));
    REQUIRE(!((Cursor *)c2)->is_nil(Cursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c2, &key, &rec, 0));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_close(c1));
    REQUIRE(0 == ham_cursor_close(c2));
  }

  void eraseMiddleDuplicateTest() {
    ham_cursor_t *c1, *c2;
    ham_key_t key;
    ham_record_t rec;
    int value = 0;
    ::memset(&key, 0, sizeof(key));

    ::memset(&rec, 0, sizeof(rec));
    value = 1;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    ::memset(&rec, 0, sizeof(rec));
    value=3;
    rec.data=&value;
    rec.size=sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    REQUIRE(0 == ham_cursor_create(&c1, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c2, m_db, 0, 0));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c1, &key, &rec, HAM_CURSOR_NEXT));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(3 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_erase(c1, 0));
    REQUIRE(((Cursor *)c1)->is_nil(Cursor::kBtree));
    REQUIRE(!((Cursor *)c2)->is_nil(Cursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_NEXT));
    REQUIRE(3 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(3 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_PREVIOUS));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_close(c1));
    REQUIRE(0 == ham_cursor_close(c2));
  }

  void eraseTinyDuplicatesTest() {
    ham_cursor_t *c;

    insertData("111", "111");
    insertData("111", "222");
    insertData("111", "333");
    insertData("111", "444");
    insertData("111", "555");

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    checkData(c, HAM_CURSOR_FIRST,  0, "111");
    REQUIRE(0 == ham_cursor_erase(c, 0));
    checkData(c, HAM_CURSOR_FIRST,  0, "222");
    REQUIRE(0 == ham_cursor_erase(c, 0));
    checkData(c, HAM_CURSOR_FIRST,  0, "333");
    REQUIRE(0 == ham_cursor_erase(c, 0));
    checkData(c, HAM_CURSOR_FIRST,  0, "444");
    REQUIRE(0 == ham_cursor_erase(c, 0));
    checkData(c, HAM_CURSOR_FIRST,  0, "555");
    REQUIRE(0 == ham_cursor_erase(c, 0));
    checkData(c, HAM_CURSOR_FIRST,  HAM_KEY_NOT_FOUND, "555");

    REQUIRE(0 == ham_cursor_close(c));
  }

  void reopenTest() {
    ham_cursor_t *c;

    insertData("000", "aaaaaaaaaa");
    insertData("111", "1111111111");
    insertData("111", "2222222222");
    insertData("111", "3333333333");
    insertData("222", "bbbbbbbbbb");

    if (!(m_flags&HAM_IN_MEMORY)) {
      /* reopen the database */
      teardown();
      REQUIRE(0 ==
          ham_env_open(&m_env, Globals::opath(".test"), m_flags, 0));
      REQUIRE(0 ==
          ham_env_open_db(m_env, &m_db, 1, 0, 0));
    }
    REQUIRE((((LocalDatabase *)m_db)->get_rt_flags() & HAM_ENABLE_DUPLICATES));

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    insertData("111", "4444444444");
    insertData("111", "5555555555");
    insertData("111", "6666666666");

    checkData(c, HAM_CURSOR_FIRST,  0, "aaaaaaaaaa");
    checkData(c, HAM_CURSOR_NEXT,   0, "1111111111");
    checkData(c, HAM_CURSOR_NEXT,   0, "2222222222");
    checkData(c, HAM_CURSOR_NEXT,   0, "3333333333");
    checkData(c, HAM_CURSOR_NEXT,   0, "4444444444");
    checkData(c, HAM_CURSOR_NEXT,   0, "5555555555");
    checkData(c, HAM_CURSOR_NEXT,   0, "6666666666");
    checkData(c, HAM_CURSOR_NEXT,   0, "bbbbbbbbbb");

    checkData(c, HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES,
            HAM_KEY_NOT_FOUND, 0);
    checkData(c, HAM_CURSOR_NEXT,
            HAM_KEY_NOT_FOUND, 0);

    REQUIRE(0 == ham_cursor_close(c));
  }

  void moveToLastDuplicateTest() {
    ham_cursor_t *c;

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    insertData(0, "3333333333");
    insertData(0, "2222222222");
    insertData(0, "1111111111");

    checkData(c, HAM_CURSOR_LAST,   0, "1111111111");

    ham_cursor_close(c);
  }

  void eraseDuplicateTest() {
    ham_cursor_t *c1, *c2;
    ham_key_t key;
    ham_record_t rec;
    int value = 1;
    ::memset(&key, 0, sizeof(key));

    ::memset(&rec, 0, sizeof(rec));
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    REQUIRE(0 == ham_cursor_create(&c1, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c2, m_db, 0, 0));

    REQUIRE(0 == ham_cursor_find(c1, &key, 0, 0));
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c1, &key, &rec, 0));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_find(c2, &key, 0, 0));
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c2, &key, &rec, 0));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_erase(c1, 0));
    REQUIRE(((Cursor *)c1)->is_nil(Cursor::kBtree));
    REQUIRE(((Cursor *)c2)->is_nil(Cursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_close(c1));
    REQUIRE(0 == ham_cursor_close(c2));
  }

  void eraseDuplicateUncoupledTest() {
    ham_cursor_t *c1, *c2;
    ham_key_t key;
    ham_record_t rec;
    int value = 0;
    ::memset(&key, 0, sizeof(key));

    ::memset(&rec, 0, sizeof(rec));
    value = 1;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    REQUIRE(0 == ham_cursor_create(&c1, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c2, m_db, 0, 0));

    REQUIRE(0 == ham_cursor_find(c1, &key, 0, 0));
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c1, &key, &rec, 0));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_find(c2, &key, 0, 0));
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c2, &key, &rec, 0));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 ==
        ((Cursor *)c1)->get_btree_cursor()->uncouple_from_page());
    REQUIRE(0 ==
        ((Cursor *)c2)->get_btree_cursor()->uncouple_from_page());
    REQUIRE(0 == ham_cursor_erase(c1, 0));
    REQUIRE(((Cursor *)c1)->is_nil(Cursor::kBtree));
    REQUIRE(((Cursor *)c2)->is_nil(Cursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_close(c1));
    REQUIRE(0 == ham_cursor_close(c2));
  }

  void eraseSecondDuplicateTest() {
    ham_cursor_t *c1, *c2;
    ham_key_t key;
    ham_record_t rec;
    int value = 1;
    ::memset(&key, 0, sizeof(key));

    ::memset(&rec, 0, sizeof(rec));
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    REQUIRE(0 == ham_cursor_create(&c1, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c2, m_db, 0, 0));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_erase(c1, 0));
    REQUIRE(((Cursor *)c1)->is_nil(Cursor::kBtree));
    REQUIRE(((Cursor *)c2)->is_nil(Cursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(1 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_close(c1));
    REQUIRE(0 == ham_cursor_close(c2));
  }

  void eraseSecondDuplicateUncoupledTest() {
    ham_cursor_t *c1, *c2;
    ham_key_t key;
    ham_record_t rec;
    int value = 1;
    ::memset(&key, 0, sizeof(key));

    ::memset(&rec, 0, sizeof(rec));
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    REQUIRE(0 == ham_cursor_create(&c1, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c2, m_db, 0, 0));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 ==
        ((Cursor *)c1)->get_btree_cursor()->uncouple_from_page());
    REQUIRE(0 ==
        ((Cursor *)c2)->get_btree_cursor()->uncouple_from_page());
    REQUIRE(0 == ham_cursor_erase(c1, 0));
    REQUIRE(((Cursor *)c1)->is_nil(Cursor::kBtree));
    REQUIRE(((Cursor *)c2)->is_nil(Cursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(1 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_close(c1));
    REQUIRE(0 == ham_cursor_close(c2));
  }

  void eraseOtherDuplicateTest() {
    ham_cursor_t *c1, *c2;
    ham_key_t key;
    ham_record_t rec;
    int value = 1;
    ::memset(&key, 0, sizeof(key));

    ::memset(&rec, 0, sizeof(rec));
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    REQUIRE(0 == ham_cursor_create(&c1, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c2, m_db, 0, 0));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
    REQUIRE(1 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_erase(c1, 0));
    REQUIRE(((Cursor *)c1)->is_nil(Cursor::kBtree));
    REQUIRE(!((Cursor *)c2)->is_nil(Cursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
    REQUIRE(2 == *(int *)rec.data);

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c2, &key, &rec, 0));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_close(c1));
    REQUIRE(0 == ham_cursor_close(c2));
  }

  void moveToPreviousDuplicateTest() {
    ham_cursor_t *c;

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

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

    checkData(c, HAM_CURSOR_LAST,   0, "xxxxxxxx");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "0000000000");

    checkData(c, HAM_CURSOR_LAST,   0, "xxxxxxxx");
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "1111111111");

    checkData(c, HAM_CURSOR_LAST,   0, "xxxxxxxx");
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_ONLY_DUPLICATES,
        HAM_KEY_NOT_FOUND, 0);

    checkData(c, HAM_CURSOR_FIRST,  0, "1111111111");
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_ONLY_DUPLICATES,
        HAM_KEY_NOT_FOUND, 0);
    checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES,
        HAM_KEY_NOT_FOUND, 0);
    checkData(c, HAM_CURSOR_PREVIOUS,
        HAM_KEY_NOT_FOUND, 0);

    ham_cursor_close(c);
  }

  void overwriteCursorTest() {
    ham_cursor_t *c;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    insertData(0, "1111111111");
    insertData(0, "2222222222");
    insertData(0, "33");
    insertData(0, "4444444444");
    insertData(0, "5555555555");

    checkData(c, HAM_CURSOR_FIRST,  0, "1111111111");
    checkData(c, HAM_CURSOR_NEXT,   0, "2222222222");
    checkData(c, HAM_CURSOR_NEXT,   0, "33");

    ::memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"3333333333333333333333333333333333333333333333333333";
    rec.size = (ham_size_t)strlen((char *)rec.data)+1;
    REQUIRE(0 == ham_cursor_overwrite(c, &rec, 0));
    checkData(c, HAM_CURSOR_FIRST,  0, "1111111111");
    checkData(c, HAM_CURSOR_NEXT,   0, "2222222222");
    checkData(c, HAM_CURSOR_NEXT,   0,
        "3333333333333333333333333333333333333333333333333333");
    checkData(c, HAM_CURSOR_NEXT,   0, "4444444444");

    ::memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"44";
    rec.size = (ham_size_t)strlen((char *)rec.data) + 1;
    REQUIRE(0 == ham_cursor_overwrite(c, &rec, 0));

    checkData(c, HAM_CURSOR_LAST,   0, "5555555555");
    checkData(c, HAM_CURSOR_PREVIOUS, 0, "44");
    checkData(c, HAM_CURSOR_PREVIOUS, 0,
        "3333333333333333333333333333333333333333333333333333");

    ham_cursor_close(c);
  }

  void overwriteMultipleCursorTest() {
    ham_cursor_t *c1, *c2, *c3;
    ham_key_t key;
    ham_record_t rec;
    ::memset(&key, 0, sizeof(key));

    REQUIRE(0 == ham_cursor_create(&c1, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c2, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c3, m_db, 0, 0));

    insertData(0, "1111111111");
    insertData(0, "2222222222");
    insertData(0, "33");
    insertData(0, "4444444444");
    insertData(0, "5555555555");

    checkData(c1, HAM_CURSOR_FIRST,  0, "1111111111");
    checkData(c1, HAM_CURSOR_NEXT,   0, "2222222222");
    checkData(c1, HAM_CURSOR_NEXT,   0, "33");
    checkData(c2, HAM_CURSOR_FIRST,  0, "1111111111");
    checkData(c3, HAM_CURSOR_FIRST,  0, "1111111111");
    checkData(c3, HAM_CURSOR_NEXT,   0, "2222222222");
    checkData(c3, HAM_CURSOR_NEXT,   0, "33");

    ::memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"3333333333333333333333333333333333333333333333333333";
    rec.size = (ham_size_t)strlen((char *)rec.data) + 1;
    REQUIRE(0 == ham_cursor_overwrite(c1, &rec, 0));
    checkData(c1, 0,           0,
        "3333333333333333333333333333333333333333333333333333");
    checkData(c2, HAM_CURSOR_FIRST,  0, "1111111111");
    checkData(c1, HAM_CURSOR_FIRST,  0, "1111111111");
    checkData(c1, HAM_CURSOR_NEXT,   0, "2222222222");
    checkData(c1, HAM_CURSOR_NEXT,   0,
        "3333333333333333333333333333333333333333333333333333");
    checkData(c3, 0,           0,
        "3333333333333333333333333333333333333333333333333333");
    checkData(c1, HAM_CURSOR_NEXT,   0, "4444444444");
    checkData(c3, HAM_CURSOR_NEXT,   0, "4444444444");

    ::memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"44";
    rec.size = (ham_size_t)strlen((char *)rec.data) + 1;
    REQUIRE(0 == ham_cursor_overwrite(c1, &rec, 0));
    checkData(c3, 0,           0, "44");
    checkData(c3, HAM_CURSOR_PREVIOUS, 0,
        "3333333333333333333333333333333333333333333333333333");
    checkData(c3, HAM_CURSOR_NEXT,   0, "44");
    checkData(c3, HAM_CURSOR_NEXT,   0, "5555555555");

    checkData(c1, HAM_CURSOR_LAST,   0, "5555555555");
    checkData(c1, HAM_CURSOR_PREVIOUS, 0, "44");
    checkData(c1, HAM_CURSOR_PREVIOUS, 0,
        "3333333333333333333333333333333333333333333333333333");
    checkData(c1, HAM_CURSOR_FIRST,  0, "1111111111");
    checkData(c2, HAM_CURSOR_FIRST,  0, "1111111111");

    ham_cursor_close(c1);
    ham_cursor_close(c2);
    ham_cursor_close(c3);
  }

  void invalidFlagsTest() {
    ham_cursor_t *c;

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_move(c, 0, 0,
            HAM_SKIP_DUPLICATES | HAM_ONLY_DUPLICATES));

    ham_cursor_close(c);
  }

  void overwriteTest() {
    ham_cursor_t *c1, *c2;
    ham_record_t rec;
    ham_key_t key;

    insertData(0, "111");
    insertData(0, "2222222222");
    insertData(0, "333");
    insertData(0, "4444444444");

    REQUIRE(0 == ham_cursor_create(&c1, m_db, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c2, m_db, 0, 0));

    checkData(c1, HAM_CURSOR_FIRST, 0, "111");
    checkData(c1, HAM_CURSOR_NEXT,  0, "2222222222");
    checkData(c2, HAM_CURSOR_FIRST, 0, "111");

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"1111111111111111111111111111111111111111";
    rec.size = (ham_size_t)strlen((char *)rec.data)+1;
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
    checkData(c2, 0,          0,
        "1111111111111111111111111111111111111111");

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    rec.data = (void *)"00";
    rec.size = (ham_size_t)strlen((char *)rec.data)+1;
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
    checkData(c2, 0,          0, "00");

    checkData(c1, HAM_CURSOR_PREVIOUS,  0, "00");
    checkData(c1, HAM_CURSOR_FIRST,   0, "00");

    ham_cursor_close(c1);
    ham_cursor_close(c2);
  }

  void endianTest() {
// i currently don't have access to a big-endian machine, and the existing
// files were created with a database < 1.0.9 and are no longer supported
#if 0
    ham_key_t key;
    ham_record_t rec;
    ham_cursor_t *c;

    /* close the existing database handle */
    REQUIRE(0 == ham_db_close(m_db, 0));

    (void)os::unlink(Globals::opath(".test"));

    /* generated with `cat ../COPYING.GPL2 | ./db5` */
#if defined(HAM_LITTLE_ENDIAN)
    REQUIRE(true ==
      os::copy(BFC_IPATH("data/dupe-endian-test-open-database-be.hdb"),
          Globals::opath(".test")));
#else
    REQUIRE(true ==
      os::copy(BFC_IPATH("data/dupe-endian-test-open-database-le.hdb"),
          Globals::opath(".test")));
#endif
    REQUIRE(0 == ham_open(m_db, Globals::opath(".test"), 0));

    memset(&key, 0, sizeof(key));
    key.data = (void *)"written";
    key.size = 8;

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    REQUIRE(0 == ham_cursor_find(c, &key, 0, 0));
    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ham_cursor_move(c, 0, &rec, 0));
    REQUIRE(_ham_byteswap32(125) == *(unsigned int *)rec.data);

    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ham_cursor_move(c, 0, &rec,
                HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES));
    REQUIRE(_ham_byteswap32(142) == *(unsigned int *)rec.data);

    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ham_cursor_move(c, 0, &rec,
                HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES));
    REQUIRE(_ham_byteswap32(235) == *(unsigned int *)rec.data);

    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ham_cursor_move(c, 0, &rec,
                HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES));
    REQUIRE(_ham_byteswap32(331) == *(unsigned int *)rec.data);

    memset(&rec, 0, sizeof(rec));
    REQUIRE(HAM_KEY_NOT_FOUND == ham_cursor_move(c, 0, &rec,
                HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES));

    REQUIRE(0 == ham_cursor_close(c));
#endif
  }

  void eraseCursorTest() {
    ham_key_t key;
    ham_cursor_t *c;

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    insertData(0, "1111111111");
    insertData(0, "2222222222");
    insertData(0, "3333333333");
    insertData(0, "4444444444");
    insertData(0, "5555555555");

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ham_cursor_erase(c, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ham_cursor_erase(c, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ham_cursor_erase(c, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ham_cursor_erase(c, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ham_cursor_erase(c, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(HAM_KEY_NOT_FOUND ==
            ham_cursor_find(c, &key, 0, 0));

    REQUIRE(0 == ham_cursor_close(c));
  }

  void insertLastTest() {
    ham_key_t key;
    ham_record_t rec;
    ham_cursor_t *c;
    const char *values[] = { "11111", "222222", "3333333", "44444444" };

    memset(&key, 0, sizeof(key));

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    for (int i = 0; i < 4; i++) {
      memset(&rec, 0, sizeof(rec));
      rec.data = (void *)values[i];
      rec.size = (ham_size_t)strlen((char *)rec.data)+1;
      REQUIRE(0 ==
            ham_cursor_insert(c, &key, &rec,
                HAM_DUPLICATE_INSERT_LAST));
      memset(&rec, 0, sizeof(rec));
      REQUIRE(0 ==
            ham_cursor_move(c, 0, &rec, 0));
      REQUIRE(strlen(values[i]) == strlen((char *)rec.data));
      REQUIRE(0 == strcmp(values[i], (char *)rec.data));
      REQUIRE((ham_size_t)i ==
          ((Cursor *)c)->get_btree_cursor()->get_duplicate_index());
    }

    checkData(c, HAM_CURSOR_FIRST, 0, values[0]);
    checkData(c, HAM_CURSOR_NEXT,  0, values[1]);
    checkData(c, HAM_CURSOR_NEXT,  0, values[2]);
    checkData(c, HAM_CURSOR_NEXT,  0, values[3]);
    checkData(c, HAM_CURSOR_NEXT,  HAM_KEY_NOT_FOUND, values[3]);

    REQUIRE(0 == ham_cursor_close(c));
  }

  void insertFirstTest() {
    ham_key_t key;
    ham_record_t rec;
    ham_cursor_t *c;
    const char *values[] = { "11111", "222222", "3333333", "44444444" };

    memset(&key, 0, sizeof(key));

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    for (int i = 0; i < 4; i++) {
      memset(&rec, 0, sizeof(rec));
      rec.data = (void *)values[i];
      rec.size = (ham_size_t)strlen((char *)rec.data) + 1;
      REQUIRE(0 ==
            ham_cursor_insert(c, &key, &rec,
                HAM_DUPLICATE_INSERT_FIRST));
      memset(&rec, 0, sizeof(rec));
      REQUIRE(0 ==
            ham_cursor_move(c, 0, &rec, 0));
      REQUIRE(strlen((char *)rec.data) == strlen(values[i]));
      REQUIRE(0 == strcmp(values[i], (char *)rec.data));
      REQUIRE((ham_size_t)0 ==
          ((Cursor *)c)->get_btree_cursor()->get_duplicate_index());
    }

    checkData(c, HAM_CURSOR_FIRST, 0, values[3]);
    checkData(c, HAM_CURSOR_NEXT,  0, values[2]);
    checkData(c, HAM_CURSOR_NEXT,  0, values[1]);
    checkData(c, HAM_CURSOR_NEXT,  0, values[0]);
    checkData(c, HAM_CURSOR_NEXT,  HAM_KEY_NOT_FOUND, values[0]);

    REQUIRE(0 == ham_cursor_close(c));
  }

  void insertAfterTest() {
    ham_key_t key;
    ham_record_t rec;
    ham_cursor_t *c;
    const char *values[] = { "11111", "222222", "3333333", "44444444" };

    memset(&key, 0, sizeof(key));

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    for (int i = 0; i < 4; i++) {
      memset(&rec, 0, sizeof(rec));
      rec.data = (void *)values[i];
      rec.size = (ham_size_t)strlen((char *)rec.data) + 1;
      REQUIRE(0 ==
            ham_cursor_insert(c, &key, &rec,
                HAM_DUPLICATE_INSERT_AFTER));
      memset(&rec, 0, sizeof(rec));
      REQUIRE(0 ==
            ham_cursor_move(c, 0, &rec, 0));
      REQUIRE(strlen((char *)rec.data) == strlen(values[i]));
      REQUIRE(0 == strcmp(values[i], (char *)rec.data));
      REQUIRE((ham_size_t)(i >= 1 ? 1 : 0) ==
            ((Cursor *)c)->get_btree_cursor()->get_duplicate_index());
      REQUIRE(0 ==
            ham_cursor_move(c, 0, 0, HAM_CURSOR_FIRST));
    }

    checkData(c, HAM_CURSOR_FIRST,  0, values[0]);
    checkData(c, HAM_CURSOR_NEXT,   0, values[3]);
    checkData(c, HAM_CURSOR_NEXT,   0, values[2]);
    checkData(c, HAM_CURSOR_NEXT,   0, values[1]);
    checkData(c, HAM_CURSOR_NEXT,   HAM_KEY_NOT_FOUND, values[0]);

    REQUIRE(0 == ham_cursor_close(c));
  }

  void insertBeforeTest() {
    ham_key_t key;
    ham_record_t rec;
    ham_cursor_t *c;
    const char *values[] = { "11111", "222222", "3333333", "44444444" };

    memset(&key, 0, sizeof(key));

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    for (int i = 0; i < 4; i++) {
      memset(&rec, 0, sizeof(rec));
      rec.data = (void *)values[i];
      rec.size = (ham_size_t)strlen((char *)rec.data) + 1;
      REQUIRE(0 ==
            ham_cursor_insert(c, &key, &rec,
                HAM_DUPLICATE_INSERT_BEFORE));
      memset(&rec, 0, sizeof(rec));
      REQUIRE(0 ==
            ham_cursor_move(c, 0, &rec, 0));
      REQUIRE(strlen((char *)rec.data) == strlen(values[i]));
      REQUIRE(0 == strcmp(values[i], (char *)rec.data));
      REQUIRE((ham_size_t)(i <= 1 ? 0 : i-1) ==
            ((Cursor *)c)->get_btree_cursor()->get_duplicate_index());
      REQUIRE(0 ==
            ham_cursor_move(c, 0, 0, HAM_CURSOR_LAST));
    }

    checkData(c, HAM_CURSOR_FIRST,  0, values[1]);
    checkData(c, HAM_CURSOR_NEXT,   0, values[2]);
    checkData(c, HAM_CURSOR_NEXT,   0, values[3]);
    checkData(c, HAM_CURSOR_NEXT,   0, values[0]);
    checkData(c, HAM_CURSOR_NEXT,   HAM_KEY_NOT_FOUND, values[0]);

    REQUIRE(0 == ham_cursor_close(c));
  }

  void overwriteVariousSizesTest() {
    ham_key_t key;
    ham_record_t rec;
    ham_cursor_t *c;
    ham_size_t sizes[] = { 0, 6, 8, 10 };
    const char *values[] = { 0, "55555", "8888888", "999999999" };
    const char *newvalues[4];

    memset(&key, 0, sizeof(key));

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

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
          REQUIRE(0 ==
              ham_cursor_insert(c, &key, &rec,
                  HAM_DUPLICATE_INSERT_LAST));
        }
        else {
          /* other rounds: just overwrite them */
          REQUIRE(0 ==
              ham_cursor_overwrite(c, &rec, 0));
          if (i != (s + 4) - 1)
            REQUIRE(0 ==
              ham_cursor_move(c, 0, 0, HAM_CURSOR_NEXT));
        }
      }

      checkData(c, HAM_CURSOR_FIRST,  0, newvalues[0]);
      checkData(c, HAM_CURSOR_NEXT,   0, newvalues[1]);
      checkData(c, HAM_CURSOR_NEXT,   0, newvalues[2]);
      checkData(c, HAM_CURSOR_NEXT,   0, newvalues[3]);
      checkData(c, HAM_CURSOR_NEXT,   HAM_KEY_NOT_FOUND, newvalues[1]);

      /* move to first element */
      checkData(c, HAM_CURSOR_FIRST,  0, newvalues[0]);
    }

    REQUIRE(0 == ham_cursor_close(c));
  }

  void getDuplicateCountTest() {
    ham_size_t count;
    ham_cursor_t *c;

    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_get_duplicate_count(0, &count, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_get_duplicate_count(c, 0, 0));
    REQUIRE(HAM_CURSOR_IS_NIL ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((ham_size_t)0 == count);

    insertData(0, "1111111111");
    checkData(c, HAM_CURSOR_NEXT,   0, "1111111111");
    REQUIRE(0 ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((ham_size_t)1 == count);

    insertData(0, "2222222222");
    checkData(c, HAM_CURSOR_NEXT,   0, "2222222222");
    REQUIRE(0 ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((ham_size_t)2 == count);

    insertData(0, "3333333333");
    checkData(c, HAM_CURSOR_NEXT,   0, "3333333333");
    REQUIRE(0 ==
        ((Cursor *)c)->get_btree_cursor()->uncouple_from_page());
    REQUIRE(0 ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((ham_size_t)3 == count);

    REQUIRE(0 == ham_cursor_erase(c, 0));
    REQUIRE(HAM_CURSOR_IS_NIL ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    checkData(c, HAM_CURSOR_FIRST,  0, "1111111111");
    REQUIRE(0 ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((ham_size_t)2 == count);

    REQUIRE(0 == ham_cursor_close(c));

    if (!(m_flags & HAM_IN_MEMORY)) {
      /* reopen the database */
      teardown();
      REQUIRE(0 == ham_env_open(&m_env, Globals::opath(".test"),
              m_flags, 0));
      REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));
      REQUIRE((((LocalDatabase *)m_db)->get_rt_flags() & HAM_ENABLE_DUPLICATES));

      REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

      checkData(c, HAM_CURSOR_NEXT,   0, "1111111111");
      REQUIRE(0 ==
          ham_cursor_get_duplicate_count(c, &count, 0));
      REQUIRE((ham_size_t)2 == count);

      REQUIRE(0 == ham_cursor_close(c));
    }
  }

  void insertManyManyTest() {
    ham_key_t key;
    ham_record_t rec;
    ham_cursor_t *c;
    ham_parameter_t params[2] = {
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };

    teardown();
    REQUIRE(0 == ham_env_create(&m_env, Globals::opath(".test"),
          m_flags, 0664, &params[0]));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 
          HAM_ENABLE_DUPLICATES, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_cursor_create(&c, m_db, 0, 0));

    for (int i = 0; i < 256; i++) {
      memset(&rec, 0, sizeof(rec));
      rec.size = sizeof(i);
      rec.data = &i;

      REQUIRE(0 ==
          ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
    }

    for (int i = 0; i < 256; i++) {
      memset(&rec, 0, sizeof(rec));

      REQUIRE(0 ==
          ham_cursor_move(c, &key, &rec, HAM_CURSOR_NEXT));
      REQUIRE((ham_size_t)4 == rec.size);
      REQUIRE(i == *(int *)rec.data);
    }

    REQUIRE(HAM_KEY_NOT_FOUND ==
        ham_cursor_move(c, 0, 0, HAM_CURSOR_NEXT));
    REQUIRE(0 == ham_cursor_close(c));
  }

  void cloneTest() {
    ham_cursor_t *c1, *c2;
    ham_key_t key;
    ham_record_t rec;
    int value = 0;
    ::memset(&key, 0, sizeof(key));

    ::memset(&rec, 0, sizeof(rec));
    value = 1;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    ::memset(&rec, 0, sizeof(rec));
    value = 2;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    ::memset(&rec, 0, sizeof(rec));
    value = 3;
    rec.data = &value;
    rec.size = sizeof(value);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

    REQUIRE(0 == ham_cursor_create(&c1, m_db, 0, 0));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
    REQUIRE(0 ==
            ham_cursor_move(c1, &key, &rec, HAM_CURSOR_NEXT));
    REQUIRE(2 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_clone(c1, &c2));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
            ham_cursor_move(c2, &key, &rec, HAM_CURSOR_NEXT));
    REQUIRE(3 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_erase(c1, 0));
    REQUIRE(((Cursor *)c1)->is_nil(Cursor::kBtree));
    REQUIRE(!((Cursor *)c2)->is_nil(Cursor::kBtree));

    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));
    REQUIRE(0 ==
        ham_cursor_move(c2, &key, &rec, 0));
    REQUIRE(3 == *(int *)rec.data);

    REQUIRE(0 == ham_cursor_close(c1));
    REQUIRE(0 == ham_cursor_close(c2));
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
 * test ham_cursor_move(... HAM_CURSOR_PREVIOUS)
 */
TEST_CASE("DuplicateFixture/moveToPreviousDuplicateTest", "")
{
  DuplicateFixture f;
  f.moveToPreviousDuplicateTest();
}

/*
 * overwrite duplicates using ham_db_insert(... HAM_OVERWRITE)
 */
TEST_CASE("DuplicateFixture/overwriteTest", "")
{
  DuplicateFixture f;
  f.overwriteTest();
}

/*
 * overwrite duplicates using ham_cursor_insert(... HAM_OVERWRITE)
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
 * reads a big-endian database (if started on a little-endian system)
 * or vice versa
 */
TEST_CASE("DuplicateFixture/endianTest", "")
{
  DuplicateFixture f;
  f.endianTest();
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
 * tests HAM_DUPLICATE_INSERT_LAST and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture/insertLastTest", "")
{
  DuplicateFixture f;
  f.insertLastTest();
}

/*
 * tests HAM_DUPLICATE_INSERT_FIRST and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture/insertFirstTest", "")
{
  DuplicateFixture f;
  f.insertFirstTest();
}

/*
 * tests HAM_DUPLICATE_INSERT_AFTER and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture/insertAfterTest", "")
{
  DuplicateFixture f;
  f.insertAfterTest();
}

/*
 * tests HAM_DUPLICATE_INSERT_BEFORE and makes sure that the cursor
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
 * insert a lot of duplicates to provoke a page-split in the duplicate
 * table
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


TEST_CASE("DuplicateFixture-inmem/invalidFlagsTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.invalidFlagsTest();
}

TEST_CASE("DuplicateFixture-inmem/insertDuplicatesTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertDuplicatesTest();
}

TEST_CASE("DuplicateFixture-inmem/overwriteDuplicatesTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.overwriteDuplicatesTest();
}

TEST_CASE("DuplicateFixture-inmem/overwriteVariousDuplicatesTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.overwriteVariousDuplicatesTest();
}

TEST_CASE("DuplicateFixture-inmem/insertMoveForwardTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertMoveForwardTest();
}

TEST_CASE("DuplicateFixture-inmem/insertMoveBackwardTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertMoveBackwardTest();
}

TEST_CASE("DuplicateFixture-inmem/insertEraseTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertEraseTest();
}

TEST_CASE("DuplicateFixture-inmem/insertTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertTest();
}

TEST_CASE("DuplicateFixture-inmem/insertSkipDuplicatesTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertSkipDuplicatesTest();
}

TEST_CASE("DuplicateFixture-inmem/insertOnlyDuplicatesTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertOnlyDuplicatesTest();
}

TEST_CASE("DuplicateFixture-inmem/insertOnlyDuplicatesTest2", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertOnlyDuplicatesTest2();
}

TEST_CASE("DuplicateFixture-inmem/coupleUncoupleTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.coupleUncoupleTest();
}

TEST_CASE("DuplicateFixture-inmem/moveToLastDuplicateTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.moveToLastDuplicateTest();
}

/*
 * insert 2 dupes, create 2 cursors (both on the first dupe).
 * delete the first cursor, make sure that both cursors are
 * NILled and the second dupe is still available
 */
TEST_CASE("DuplicateFixture-inmem/eraseDuplicateTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.eraseDuplicateTest();
}

/*
 * same as above, but uncouples the cursor before the first cursor
 * is deleted
 */
TEST_CASE("DuplicateFixture-inmem/eraseDuplicateUncoupledTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.eraseDuplicateUncoupledTest();
}

/*
 * insert 2 dupes, create 2 cursors (both on the second dupe).
 * delete the first cursor, make sure that both cursors are
 * NILled and the first dupe is still available
 */
TEST_CASE("DuplicateFixture-inmem/eraseSecondDuplicateTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.eraseSecondDuplicateTest();
}

/*
 * same as above, but uncouples the cursor before the second cursor
 * is deleted
 */
TEST_CASE("DuplicateFixture-inmem/eraseSecondDuplicateUncoupledTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.eraseSecondDuplicateUncoupledTest();
}

/*
 * insert 2 dupes, create 2 cursors (one on the first, the other on the
 * second dupe). delete the first cursor, make sure that it's NILled
 * and the other cursor is still valid.
 */
TEST_CASE("DuplicateFixture-inmem/eraseOtherDuplicateTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.eraseOtherDuplicateTest();
}

/*
 * same as above, but uncouples the cursor before the second cursor
 * is deleted
 */
TEST_CASE("DuplicateFixture-inmem/eraseOtherDuplicateUncoupledTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.eraseOtherDuplicateUncoupledTest();
}

/*
 * inserts 3 dupes, creates 2 cursors on the middle item; delete the
 * first cursor, make sure that the second is NILled and that the first
 * and last item still exists
 */
TEST_CASE("DuplicateFixture-inmem/eraseMiddleDuplicateTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.eraseMiddleDuplicateTest();
}

/*
 * inserts a few TINY dupes, then erases them all but the last element
 */
TEST_CASE("DuplicateFixture-inmem/eraseTinyDuplicatesTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.eraseTinyDuplicatesTest();
}

/*
 * inserts a few duplicates, reopens the database; continues inserting
 */
TEST_CASE("DuplicateFixture-inmem/reopenTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.reopenTest();
}

/*
 * test ham_cursor_move(... HAM_CURSOR_PREVIOUS)
 */
TEST_CASE("DuplicateFixture-inmem/moveToPreviousDuplicateTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.moveToPreviousDuplicateTest();
}

/*
 * overwrite duplicates using ham_db_insert(... HAM_OVERWRITE)
 */
TEST_CASE("DuplicateFixture-inmem/overwriteTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.overwriteTest();
}

/*
 * overwrite duplicates using ham_cursor_insert(... HAM_OVERWRITE)
 */
TEST_CASE("DuplicateFixture-inmem/overwriteCursorTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.overwriteCursorTest();
}

/*
 * same as overwriteCursorTest, but uses multiple cursors and makes
 * sure that their positions are not modified
 */
TEST_CASE("DuplicateFixture-inmem/overwriteMultipleCursorTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.overwriteMultipleCursorTest();
}

/*
 * reads a big-endian database (if started on a little-endian system)
 * or vice versa
 */
TEST_CASE("DuplicateFixture-inmem/endianTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.endianTest();
}

/*
 * insert a few duplicate items, then delete them all with a cursor
 */
TEST_CASE("DuplicateFixture-inmem/eraseCursorTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.eraseCursorTest();
}

/*
 * tests HAM_DUPLICATE_INSERT_LAST and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture-inmem/insertLastTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertLastTest();
}

/*
 * tests HAM_DUPLICATE_INSERT_FIRST and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture-inmem/insertFirstTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertFirstTest();
}

/*
 * tests HAM_DUPLICATE_INSERT_AFTER and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture-inmem/insertAfterTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertAfterTest();
}

/*
 * tests HAM_DUPLICATE_INSERT_BEFORE and makes sure that the cursor
 * always points to the inserted duplicate
 */
TEST_CASE("DuplicateFixture-inmem/insertBeforeTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertBeforeTest();
}

/*
 * overwrite NULL-, TINY- and SMALL-duplicates with other
 * NULL-, TINY- and SMALL-duplicates
 */
TEST_CASE("DuplicateFixture-inmem/overwriteVariousSizesTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.overwriteVariousSizesTest();
}

/*
 * tests get_cuplicate_count
 */
TEST_CASE("DuplicateFixture-inmem/getDuplicateCountTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.getDuplicateCountTest();
}

/*
 * insert a lot of duplicates to provoke a page-split in the duplicate
 * table
 */
TEST_CASE("DuplicateFixture-inmem/insertManyManyTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.insertManyManyTest();
}

/*
 * insert several duplicates; then set a cursor to the 2nd duplicate.
 * clone the cursor, move it to the next element. then erase the
 * first cursor.
 */
TEST_CASE("DuplicateFixture-inmem/cloneTest", "")
{
  DuplicateFixture f(HAM_IN_MEMORY);
  f.cloneTest();
}

} // namespace hamsterdb
