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
#include <algorithm>

#include "3rdparty/catch/catch.hpp"

#include "globals.h"
#include "os.hpp"

#include "../src/db_local.h"
#include "../src/btree_index.h"

namespace hamsterdb {

static int g_split_count = 0;
extern void (*g_BTREE_INSERT_SPLIT_HOOK)(void);

static void
split_hook()
{
  g_split_count++;
}

#define BUFFER 128

struct BtreeDefaultFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  ham_u32_t m_key_size;
  bool m_duplicates;

  typedef std::vector<int> IntVector;

  BtreeDefaultFixture(bool duplicates = false,
                  ham_u16_t key_size = HAM_KEY_SIZE_UNLIMITED,
                  ham_u32_t page_size = 1024 * 16)
    : m_db(0), m_env(0), m_key_size(key_size), m_duplicates(duplicates) {
    os::unlink(Globals::opath(".test"));
    ham_parameter_t p1[] = {
      { HAM_PARAM_PAGESIZE, page_size },
      { 0, 0 }
    };
    ham_parameter_t p2[] = {
      { HAM_PARAM_KEY_SIZE, key_size },
      { 0, 0 }
    };
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), 0, 0644, &p1[0]));

    ham_u32_t flags = 0;
    if (duplicates)
      flags |= HAM_ENABLE_DUPLICATES;

    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, flags, &p2[0]));
  }

  ~BtreeDefaultFixture() {
    teardown();
  }

  void teardown() {
    if (m_env)
	  REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  ham_key_t makeKey(int i, char *buffer) {
    sprintf(buffer, "%08d", i);
    ham_key_t key = {0};
    key.data = &buffer[0];
    if (m_key_size != HAM_KEY_SIZE_UNLIMITED)
      key.size = m_key_size;
    else
      key.size = std::min(BUFFER, 10 + ((i % 30) * 3));
    return (key);
  }

  void insertCursorTest(const IntVector &inserts) {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    char buffer[BUFFER] = {0};

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      key = makeKey(*it, buffer);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec,
                              m_duplicates ? HAM_DUPLICATE : 0));
    }

    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      ham_key_t expected = makeKey(*it, buffer);
      REQUIRE(0 == ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
      REQUIRE(0 == strcmp((const char *)key.data, buffer));
      REQUIRE(0 == rec.size);
      REQUIRE(key.size == expected.size);
    }

    // this leaks a cursor structure, which will be cleaned up later
    // in teardown()
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
    for (IntVector::const_reverse_iterator it = inserts.rbegin();
            it != inserts.rend(); it++) {
      ham_key_t expected = makeKey(*it, buffer);
      REQUIRE(0 == ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_PREVIOUS));
      REQUIRE(0 == rec.size);
      REQUIRE(0 == strcmp((const char *)key.data, buffer));
      REQUIRE(key.size == expected.size);
    }
  }

  void insertExtendedTest(const IntVector &inserts) {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    char buffer[512] = {0};

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      key = makeKey(*it, buffer);
      key.size = sizeof(buffer);
      rec.data = key.data;
      rec.size = key.size;
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      ham_key_t key = makeKey(*it, buffer);
      key.size = sizeof(buffer);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
      REQUIRE(key.size == rec.size);
      REQUIRE(0 == memcmp(key.data, rec.data, rec.size));
    }
  }

  void eraseExtendedTest(const IntVector &inserts) {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    char buffer[512] = {0};

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      key = makeKey(*it, buffer);
      key.size = sizeof(buffer);
      REQUIRE(0 == ham_db_erase(m_db, 0, &key, 0));
      //REQUIRE(0 == ham_db_check_integrity(m_db, 0));
    }

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      ham_key_t key = makeKey(*it, buffer);
      key.size = sizeof(buffer);
      REQUIRE(HAM_KEY_NOT_FOUND == ham_db_find(m_db, 0, &key, &rec, 0));
      //REQUIRE(0 == ham_db_check_integrity(m_db, 0));
    }
  }

  void eraseCursorTest(const IntVector &inserts) {
    ham_key_t key = {0};
    char buffer[BUFFER] = {0};

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      key = makeKey(*it, buffer);
      REQUIRE(0 == ham_db_erase(m_db, 0, &key, 0));
    }

    ham_u64_t keycount = 1;
    REQUIRE(0 == ham_db_get_key_count(m_db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);
  }

  void insertFindTest(const IntVector &inserts) {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    char buffer[BUFFER] = {0};

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      key = makeKey(*it, buffer);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec,
                              m_duplicates ? HAM_DUPLICATE : 0));
    }

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      key = makeKey(*it, buffer);
      REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
      REQUIRE(0 == rec.size);
    }
  }

  void insertSplitTest(IntVector &inserts, bool test_find, bool test_cursor) {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    char buffer[BUFFER] = {0};

    g_BTREE_INSERT_SPLIT_HOOK = split_hook;
    g_split_count = 0;
    int inserted = 0;

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++, inserted++) {
      key = makeKey(*it, buffer);
      rec.data = key.data;
      rec.size = key.size;
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
      if (m_duplicates) {
        REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
      }
      if (g_split_count == 3) {
        inserts.resize(inserted + 1);
        break;
      }
      //REQUIRE(0 == ham_db_check_integrity(m_db, 0));
    }

    if (test_find) {
      for (IntVector::const_iterator it = inserts.begin();
              it != inserts.end(); it++) {
        key = makeKey(*it, buffer);
        REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
        REQUIRE(rec.size == key.size);
        REQUIRE(0 == memcmp(rec.data, key.data, key.size));
      }
    }

    if (test_cursor) {
      ham_cursor_t *cursor;

      REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
      for (IntVector::const_iterator it = inserts.begin();
              it != inserts.end(); it++) {
        ham_key_t expected = makeKey(*it, buffer);
        REQUIRE(0 == ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
        REQUIRE(0 == strcmp((const char *)key.data, buffer));
        REQUIRE(key.size == expected.size);
        REQUIRE(rec.size == key.size);
        REQUIRE(0 == memcmp(rec.data, key.data, key.size));
      }

      // this leaks a cursor structure, which will be cleaned up later
      // in teardown()
      REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));
      for (IntVector::const_reverse_iterator it = inserts.rbegin();
              it != inserts.rend(); it++) {
        ham_key_t expected = makeKey(*it, buffer);
        REQUIRE(0 == ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_PREVIOUS));
        REQUIRE(0 == strcmp((const char *)key.data, buffer));
        REQUIRE(key.size == expected.size);
        REQUIRE(rec.size == key.size);
        REQUIRE(0 == memcmp(rec.data, key.data, key.size));
      }
    }
  }
};

TEST_CASE("BtreeDefault/insertCursorTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 30; i++)
    ivec.push_back(i);

  BtreeDefaultFixture f;
  f.insertCursorTest(ivec);
}

TEST_CASE("BtreeDefault/eraseCursorTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 30; i++)
    ivec.push_back(i);

  BtreeDefaultFixture f;
  f.insertCursorTest(ivec);
  f.eraseCursorTest(ivec);
}

TEST_CASE("BtreeDefault/insertSplitTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 10000; i++)
    ivec.push_back(i);

  BtreeDefaultFixture f;
  f.insertSplitTest(ivec, true, true);
}

TEST_CASE("BtreeDefault/eraseMergeTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 10000; i++)
    ivec.push_back(i);

  BtreeDefaultFixture f;
  f.insertSplitTest(ivec, true, true);
  f.eraseCursorTest(ivec);
}

TEST_CASE("BtreeDefault/randomInsertTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 30; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducable
  std::random_shuffle(ivec.begin(), ivec.end());

  BtreeDefaultFixture f;
  f.insertFindTest(ivec);
}

TEST_CASE("BtreeDefault/eraseInsertTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 30; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducable
  std::random_shuffle(ivec.begin(), ivec.end());

  BtreeDefaultFixture f;
  f.insertFindTest(ivec);
  f.eraseCursorTest(ivec);
}

TEST_CASE("BtreeDefault/randomSplitTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 10000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducable
  std::random_shuffle(ivec.begin(), ivec.end());

  BtreeDefaultFixture f;
  f.insertSplitTest(ivec, true, false);
  f.eraseCursorTest(ivec);
}

TEST_CASE("BtreeDefault/randomEraseMergeTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 10000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducable
  std::random_shuffle(ivec.begin(), ivec.end());

  BtreeDefaultFixture f;
  f.insertSplitTest(ivec, true, false);
  f.eraseCursorTest(ivec);
}

TEST_CASE("BtreeDefault/insertDuplicatesTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 30; i++) {
    ivec.push_back(i);
    ivec.push_back(i);
  }

  BtreeDefaultFixture f(true);
  f.insertCursorTest(ivec);

#ifdef HAVE_GCC_ABI_DEMANGLE
  std::string abi;
  abi = ((LocalDatabase *)f.m_db)->get_btree_index()->test_get_classname();
  REQUIRE(abi == "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::DefaultNodeLayout<hamsterdb::DefaultLayoutImpl<unsigned short>, hamsterdb::DefaultInlineRecordImpl<hamsterdb::DefaultLayoutImpl<unsigned short> > >, hamsterdb::VariableSizeCompare>");
#endif
}

TEST_CASE("BtreeDefault/randomEraseMergeDuplicateTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 10000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducable
  std::random_shuffle(ivec.begin(), ivec.end());

  BtreeDefaultFixture f(true);
  f.insertSplitTest(ivec, true, false);
  f.eraseCursorTest(ivec);
}

TEST_CASE("BtreeDefault/insertExtendedKeyTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 100; i++)
    ivec.push_back(i);

  BtreeDefaultFixture f(true);
  f.insertExtendedTest(ivec);
}

TEST_CASE("BtreeDefault/insertExtendedKeySplitTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 1000; i++)
    ivec.push_back(i);

  g_BTREE_INSERT_SPLIT_HOOK = split_hook;
  g_split_count = 0;
  BtreeDefaultFixture f(true);
  f.insertExtendedTest(ivec);
  REQUIRE(g_split_count == 1);
}

TEST_CASE("BtreeDefault/insertRandomExtendedKeySplitTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 1000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducable
  std::random_shuffle(ivec.begin(), ivec.end());

  g_BTREE_INSERT_SPLIT_HOOK = split_hook;
  g_split_count = 0;
  BtreeDefaultFixture f(true);
  f.insertExtendedTest(ivec);
  REQUIRE(g_split_count == 1);
}

TEST_CASE("BtreeDefault/eraseExtendedKeyTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 100; i++)
    ivec.push_back(i);

  BtreeDefaultFixture f(true);
  f.insertExtendedTest(ivec);
  f.eraseExtendedTest(ivec);
}

TEST_CASE("BtreeDefault/eraseExtendedKeySplitTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 1000; i++)
    ivec.push_back(i);

  g_BTREE_INSERT_SPLIT_HOOK = split_hook;
  g_split_count = 0;
  BtreeDefaultFixture f(true);
  f.insertExtendedTest(ivec);
  REQUIRE(g_split_count == 1);
  f.eraseExtendedTest(ivec);
}

TEST_CASE("BtreeDefault/eraseReverseExtendedKeySplitTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 1000; i++)
    ivec.push_back(i);

  g_BTREE_INSERT_SPLIT_HOOK = split_hook;
  g_split_count = 0;
  BtreeDefaultFixture f(true);
  f.insertExtendedTest(ivec);
  REQUIRE(g_split_count == 1);
  std::reverse(ivec.begin(), ivec.end());
  f.eraseExtendedTest(ivec);
}

TEST_CASE("BtreeDefault/eraseRandomExtendedKeySplitTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 1000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducable
  std::random_shuffle(ivec.begin(), ivec.end());

  g_BTREE_INSERT_SPLIT_HOOK = split_hook;
  g_split_count = 0;
  BtreeDefaultFixture f(true);
  f.insertExtendedTest(ivec);
  REQUIRE(g_split_count == 1);
  f.eraseExtendedTest(ivec);
}

TEST_CASE("BtreeDefault/eraseReverseKeySplitTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 1000; i++)
    ivec.push_back(i);

  g_BTREE_INSERT_SPLIT_HOOK = split_hook;
  g_split_count = 0;
  BtreeDefaultFixture f(true);
  f.insertCursorTest(ivec);
  REQUIRE(g_split_count == 4);
  std::reverse(ivec.begin(), ivec.end());
  f.eraseCursorTest(ivec);
}

} // namespace hamsterdb
