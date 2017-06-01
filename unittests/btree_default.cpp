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
#include <algorithm>

#include "3rdparty/catch/catch.hpp"

#include "3btree/btree_index_factory.h"
#include "4db/db_local.h"
#include "4context/context.h"

#include "os.hpp"
#include "fixture.hpp"

namespace upscaledb {

static int g_split_count = 0;
extern void (*g_BTREE_INSERT_SPLIT_HOOK)(void);

static void
split_hook()
{
  g_split_count++;
}

#define BUFFER 128

struct BtreeDefaultFixture : BaseFixture {
  uint32_t key_size;
  uint32_t record_size;
  bool use_duplicates;

  typedef std::vector<int> IntVector;

  BtreeDefaultFixture(bool duplicates = false,
                  uint16_t key_size_ = UPS_KEY_SIZE_UNLIMITED,
                  uint32_t record_size_ = UPS_RECORD_SIZE_UNLIMITED,
                  uint32_t page_size = 1024 * 16)
    : key_size(key_size_), record_size(record_size_),
        use_duplicates(duplicates) {
    ups_parameter_t p1[] = {
      { UPS_PARAM_PAGESIZE, page_size },
      { 0, 0 }
    };

    uint64_t type = key_size == 4 ? UPS_TYPE_UINT32 : UPS_TYPE_BINARY;
    ups_parameter_t p2[] = {
      { UPS_PARAM_KEY_SIZE, key_size },
      { UPS_PARAM_KEY_TYPE, type },
      { UPS_PARAM_RECORD_SIZE, record_size },
      { 0, 0 }
    };

    uint32_t flags = 0;
    if (duplicates)
      flags |= UPS_ENABLE_DUPLICATES;

    require_create(0, p1, flags, p2);
  }

  ups_key_t makeKey(int i, char *buffer) {
    sprintf(buffer, "%08d", i);
    ups_key_t key = {0};
    key.data = &buffer[0];
    if (key_size != UPS_KEY_SIZE_UNLIMITED)
      key.size = key_size;
    else
      key.size = std::min(BUFFER, 10 + ((i % 30) * 3));
    return key;
  }

  void insertCursorTest(const IntVector &inserts) {
    ups_key_t key = {0};
    ups_record_t rec = {0};
    char buffer[BUFFER] = {0};

    int i = 0;
    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++, i++) {
      key = makeKey(*it, buffer);
      if (record_size != UPS_RECORD_SIZE_UNLIMITED) {
        rec.data = &buffer[0];
        rec.size = record_size;
      }
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec,
                              use_duplicates ? UPS_DUPLICATE : 0));

#if 0
      ups_cursor_t *cursor = 0; int j = 0;
      REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
      for (IntVector::const_iterator it2 = inserts.begin();
              j <= i; it2++, j++) {
        makeKey(*it2, buffer);
        REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));
        if (key_size != UPS_KEY_SIZE_UNLIMITED)
          REQUIRE(0 == memcmp((const char *)key.data, buffer, key_size));
        else
          REQUIRE(0 == strcmp((const char *)key.data, buffer));
      }
      ups_cursor_close(cursor);
#endif
    }

    ups_cursor_t *cursor = 0;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      ups_key_t expected = makeKey(*it, buffer);
      REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));
      if (key_size != UPS_KEY_SIZE_UNLIMITED)
        REQUIRE(0 == memcmp((const char *)key.data, buffer, key_size));
      else
        REQUIRE(0 == strcmp((const char *)key.data, buffer));
      if (record_size != UPS_RECORD_SIZE_UNLIMITED)
        REQUIRE(record_size == rec.size);
      else
        REQUIRE(0 == rec.size);
      REQUIRE(key.size == expected.size);
    }

    ups_cursor_close(cursor);

    // now loop again, but in reverse order
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    for (IntVector::const_reverse_iterator it = inserts.rbegin();
            it != inserts.rend(); it++) {
      ups_key_t expected = makeKey(*it, buffer);
      REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_PREVIOUS));
      if (key_size != UPS_KEY_SIZE_UNLIMITED)
        REQUIRE(0 == memcmp((const char *)key.data, buffer, key_size));
      else
        REQUIRE(0 == strcmp((const char *)key.data, buffer));
      if (record_size != UPS_RECORD_SIZE_UNLIMITED)
        REQUIRE(record_size == rec.size);
      else
        REQUIRE(0 == rec.size);
      REQUIRE(key.size == expected.size);
    }
  }

  void insertExtendedTest(const IntVector &inserts) {
    ups_key_t key = {0};
    ups_record_t rec = {0};
    char buffer[512] = {0};

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      key = makeKey(*it, buffer);
      key.size = sizeof(buffer);
      rec.data = key.data;
      rec.size = key.size;
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      // REQUIRE(0 == ups_db_check_integrity(db, 0));
    }

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      ups_key_t key = makeKey(*it, buffer);
      key.size = sizeof(buffer);
      REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
      REQUIRE(key.size == rec.size);
      REQUIRE(0 == memcmp(key.data, rec.data, rec.size));
    }
  }

  void eraseExtendedTest(const IntVector &inserts) {
    ups_key_t key = {0};
    ups_record_t rec = {0};
    char buffer[512] = {0};

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      key = makeKey(*it, buffer);
      key.size = sizeof(buffer);
      REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
      //REQUIRE(0 == ups_db_check_integrity(db, 0));
    }

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      ups_key_t key = makeKey(*it, buffer);
      key.size = sizeof(buffer);
      REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, 0, &key, &rec, 0));
      //REQUIRE(0 == ups_db_check_integrity(db, 0));
    }
  }

  void eraseCursorTest(const IntVector &inserts) {
    ups_key_t key = {0};
    ups_cursor_t *cursor;
    char buffer[BUFFER] = {0};

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      key = makeKey(*it, buffer);
      REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));
      REQUIRE(0 == ups_cursor_erase(cursor, 0));
    }

    ups_cursor_close(cursor);

    uint64_t keycount = 1;
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);
  }

  void insertFindTest(const IntVector &inserts) {
    ups_key_t key = {0};
    ups_record_t rec = {0};
    char buffer[BUFFER] = {0};

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      key = makeKey(*it, buffer);
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec,
                              use_duplicates ? UPS_DUPLICATE : 0));
    }

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++) {
      key = makeKey(*it, buffer);
      REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
      REQUIRE(0 == rec.size);
    }
  }

  void insertSplitTest(IntVector &inserts, bool test_find, bool test_cursor) {
    ups_key_t key = {0};
    ups_record_t rec = {0};
    char buffer[BUFFER] = {0};

    g_BTREE_INSERT_SPLIT_HOOK = split_hook;
    g_split_count = 0;
    int inserted = 0;

    for (IntVector::const_iterator it = inserts.begin();
            it != inserts.end(); it++, inserted++) {
      key = makeKey(*it, buffer);
      rec.data = key.data;
      rec.size = key.size;
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec,
                              use_duplicates ? UPS_DUPLICATE : 0));

      if (g_split_count == 3) {
        inserts.resize(inserted + 1);
        break;
      }
      //REQUIRE(0 == ups_db_check_integrity(db, 0));
    }

    if (test_find) {
      for (IntVector::const_iterator it = inserts.begin();
              it != inserts.end(); it++) {
        key = makeKey(*it, buffer);
        REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
        REQUIRE(rec.size == key.size);
        REQUIRE(0 == memcmp(rec.data, key.data, key.size));
      }
    }

    if (test_cursor) {
      ups_cursor_t *cursor;

      REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
      for (IntVector::const_iterator it = inserts.begin();
              it != inserts.end(); it++) {
        ups_key_t expected = makeKey(*it, buffer);
        REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));
        REQUIRE(0 == strcmp((const char *)key.data, buffer));
        REQUIRE(key.size == expected.size);
        REQUIRE(rec.size == key.size);
        REQUIRE(0 == memcmp(rec.data, key.data, key.size));
      }

      // this leaks a cursor structure, which will be cleaned up later
      // in teardown()
      REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
      for (IntVector::const_reverse_iterator it = inserts.rbegin();
              it != inserts.rend(); it++) {
        ups_key_t expected = makeKey(*it, buffer);
        REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_PREVIOUS));
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
    ivec.push_back(i);
    ivec.push_back(i);
    ivec.push_back(i);
    ivec.push_back(i);
  }

  BtreeDefaultFixture f(true);
  f.insertCursorTest(ivec);

#ifdef HAVE_GCC_ABI_DEMANGLE
  std::string abi = f.btree_index()->test_get_classname();
  REQUIRE(abi == "upscaledb::BtreeIndexTraitsImpl<upscaledb::DefaultNodeImpl<upscaledb::VariableLengthKeyList, upscaledb::DuplicateDefaultRecordList>, upscaledb::VariableSizeCompare>");
#endif
}

TEST_CASE("BtreeDefault/randomEraseMergeDuplicateTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 10000; i++) {
    ivec.push_back(i);
    ivec.push_back(i);
    ivec.push_back(i);
  }
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

  BtreeDefaultFixture f;
  f.insertExtendedTest(ivec);
}

TEST_CASE("BtreeDefault/insertExtendedKeySplitTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 1000; i++)
    ivec.push_back(i);

  g_BTREE_INSERT_SPLIT_HOOK = split_hook;
  g_split_count = 0;
  BtreeDefaultFixture f;
  f.insertExtendedTest(ivec);
  //REQUIRE(g_split_count == 1); TODO
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
  BtreeDefaultFixture f;
  f.insertExtendedTest(ivec);
  //REQUIRE(g_split_count == 1); TODO
}

TEST_CASE("BtreeDefault/eraseExtendedKeyTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 1000; i++)
    ivec.push_back(i);

  BtreeDefaultFixture f;
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
  BtreeDefaultFixture f;
  f.insertExtendedTest(ivec);
  //REQUIRE(g_split_count == 1); TODO
  f.eraseExtendedTest(ivec);
}

TEST_CASE("BtreeDefault/eraseReverseExtendedKeySplitTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 1000; i++)
    ivec.push_back(i);

  g_BTREE_INSERT_SPLIT_HOOK = split_hook;
  g_split_count = 0;
  BtreeDefaultFixture f;
  f.insertExtendedTest(ivec);
  //REQUIRE(g_split_count == 1); TODO
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
  BtreeDefaultFixture f;
  f.insertExtendedTest(ivec);
  //REQUIRE(g_split_count == 1); TODO
  f.eraseExtendedTest(ivec);
}

TEST_CASE("BtreeDefault/eraseReverseKeySplitTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 1000; i++)
    ivec.push_back(i);

  g_BTREE_INSERT_SPLIT_HOOK = split_hook;
  g_split_count = 0;
  BtreeDefaultFixture f;
  f.insertCursorTest(ivec);
  //REQUIRE(g_split_count == 4);TODO
  std::reverse(ivec.begin(), ivec.end());
  f.eraseCursorTest(ivec);
}

TEST_CASE("BtreeDefault/varKeysFixedRecordsTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 100; i++)
    ivec.push_back(i);

  BtreeDefaultFixture f(false, UPS_KEY_SIZE_UNLIMITED, 5);
  f.insertCursorTest(ivec);

#ifdef HAVE_GCC_ABI_DEMANGLE
  std::string abi = f.btree_index()->test_get_classname();
  REQUIRE(abi == "upscaledb::BtreeIndexTraitsImpl<upscaledb::DefaultNodeImpl<upscaledb::VariableLengthKeyList, upscaledb::InlineRecordList>, upscaledb::VariableSizeCompare>");
#endif
}

TEST_CASE("BtreeDefault/fixedKeysAndRecordsWithDuplicatesTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 100; i++) {
    ivec.push_back(i);
    ivec.push_back(i);
    ivec.push_back(i);
  }

  BtreeDefaultFixture f(true, 4, 5);

#ifdef HAVE_GCC_ABI_DEMANGLE
  std::string abi = f.btree_index()->test_get_classname();
  REQUIRE(abi == "upscaledb::BtreeIndexTraitsImpl<upscaledb::DefaultNodeImpl<upscaledb::PodKeyList<unsigned int>, upscaledb::DuplicateInlineRecordList>, upscaledb::NumericCompare<unsigned int> >");
#endif

  f.insertCursorTest(ivec);
  f.eraseCursorTest(ivec);
}

TEST_CASE("BtreeDefault/fixedRecordsWithDuplicatesTest", "")
{
  BtreeDefaultFixture::IntVector ivec;
  for (int i = 0; i < 100; i++) {
    ivec.push_back(i);
    ivec.push_back(i);
    ivec.push_back(i);
  }

  BtreeDefaultFixture f(true, UPS_KEY_SIZE_UNLIMITED, 5);

#ifdef HAVE_GCC_ABI_DEMANGLE
  std::string abi = f.btree_index()->test_get_classname();
  REQUIRE(abi == "upscaledb::BtreeIndexTraitsImpl<upscaledb::DefaultNodeImpl<upscaledb::VariableLengthKeyList, upscaledb::DuplicateInlineRecordList>, upscaledb::VariableSizeCompare>");
#endif

  f.insertCursorTest(ivec);
  f.eraseCursorTest(ivec);
}


struct DuplicateTableFixture : BaseFixture {
  ScopedPtr<Context> context;

  DuplicateTableFixture(uint32_t env_flags) {
    require_create(env_flags,nullptr, UPS_ENABLE_DUPLICATES, nullptr);
    context.reset(new Context(lenv(), 0, ldb()));
  }

  ~DuplicateTableFixture() {
    teardown();
  }

  void teardown() {
    context->changeset.clear();
    close();
  }

  void createReopenTest(bool inline_records, size_t fixed_record_size,
                  const uint8_t *record_data, const size_t *record_sizes,
                  size_t num_records) {
    DuplicateTable dt(ldb(), inline_records, fixed_record_size);
    uint64_t table_id = dt.create(context.get(), record_data, num_records);
    REQUIRE(table_id != 0u);
    REQUIRE(dt.record_count() == (int)num_records);
    REQUIRE(dt.record_capacity() == (int)num_records * 2);

    ByteArray arena(fixed_record_size != UPS_RECORD_SIZE_UNLIMITED
                        ? fixed_record_size
                        : 1024);
    ups_record_t record = {0};
    record.data = arena.data();

    const uint8_t *p = record_data;
    for (size_t i = 0; i < num_records; i++) {
      dt.record(context.get(), &arena, &record, 0, i);
      REQUIRE(record.size == record_sizes[i]);

      // this test does not compare record contents if they're not
      // inline; don't see much benefit to do this, and it would only add
      // complexity
      if (!inline_records)
        p++; // skip flags
      else
        REQUIRE(0 == ::memcmp(record.data, p, record_sizes[i]));
      p += fixed_record_size != UPS_RECORD_SIZE_UNLIMITED
              ? record_sizes[i]
              : 8;
    }

    // clean up
    dt.erase_record(context.get(), 0, true);
  }

  void insertAscendingTest(bool fixed_records, size_t record_size) {
    DuplicateTable dt(ldb(), fixed_records && record_size <= 8,
                    record_size <= 8 ? record_size : UPS_RECORD_SIZE_UNLIMITED);

    const int num_records = 100;

    // create an empty table
    dt.create(context.get(), 0, 0);
    REQUIRE(dt.record_count() == 0);
    REQUIRE(dt.record_capacity() == 0);

    // fill it
    ups_record_t record = {0};
    char buffer[1024] = {0};
    record.data = &buffer[0];
    record.size = (uint32_t)record_size;
    for (int i = 0; i < num_records; i++) {
      *(size_t *)&buffer[0] = (size_t)i;
      dt.set_record(context.get(), i, &record, 0, 0);
    }

    REQUIRE(dt.record_count() == num_records);
    REQUIRE(dt.record_capacity() == 128);

    ByteArray arena(1024);
    record.data = arena.data();

    for (int i = 0; i < num_records; i++) {
      *(size_t *)&buffer[0] = (size_t)i;

      dt.record(context.get(), &arena, &record, 0, i);
      REQUIRE(record.size == record_size);
      REQUIRE(0 == memcmp(record.data, &buffer[0], record_size));
    }

    // clean up
    dt.erase_record(context.get(), 0, true);
  }

  void insertDescendingTest(bool fixed_records, size_t record_size) {
    DuplicateTable dt(ldb(), fixed_records && record_size <= 8,
                    record_size <= 8 ? record_size : UPS_RECORD_SIZE_UNLIMITED);

    const int num_records = 100;

    // create an empty table
    dt.create(context.get(), 0, 0);
    REQUIRE(dt.record_count() == 0);
    REQUIRE(dt.record_capacity() == 0);

    // fill it
    ups_record_t record = {0};
    char buffer[1024] = {0};
    record.data = &buffer[0];
    record.size = (uint32_t)record_size;
    for (int i = num_records; i > 0; i--) {
      *(size_t *)&buffer[0] = i;
      uint32_t new_index = 0;
      dt.set_record(context.get(), 0, &record, UPS_DUPLICATE_INSERT_FIRST,
                      &new_index);
      REQUIRE(new_index == 0);
    }

    REQUIRE(dt.record_count() == num_records);
    REQUIRE(dt.record_capacity() == 128);

    ByteArray arena(1024);
    record.data = arena.data();

    for (int i = num_records; i > 0; i--) {
      *(size_t *)&buffer[0] = i;

      dt.record(context.get(), &arena, &record, 0, i - 1);
      REQUIRE(record.size == record_size);
      REQUIRE(0 == ::memcmp(record.data, &buffer[0], record_size));
    }

    // clean up
    dt.erase_record(context.get(), 0, true);
  }

  void insertRandomTest(bool fixed_records, size_t record_size) {
    DuplicateTable dt(ldb(), fixed_records && record_size <= 8,
                    record_size <= 8 ? record_size : UPS_RECORD_SIZE_UNLIMITED);

    const int num_records = 100;

    // create an empty table
    dt.create(context.get(), 0, 0);
    REQUIRE(dt.record_count() == 0);
    REQUIRE(dt.record_capacity() == 0);

    // the model stores the records that we inserted
    std::vector<std::vector<uint8_t> > model;

    // fill it
    ups_record_t record = {0};
    uint8_t buf[1024] = {0};
    record.data = &buf[0];
    record.size = (uint32_t)record_size;
    for (int i = 0; i < num_records; i++) {
      *(size_t *)&buf[0] = i;
      if (i == 0) {
        dt.set_record(context.get(), i, &record, UPS_DUPLICATE_INSERT_FIRST, 0);
        model.push_back(std::vector<uint8_t>(&buf[0], &buf[record_size]));
      }
      else {
        size_t position = rand() % i;
        dt.set_record(context.get(), position, &record,
                        UPS_DUPLICATE_INSERT_BEFORE, 0);
        model.insert(model.begin() + position,
                        std::vector<uint8_t>(&buf[0], &buf[record_size]));
      }
    }

    REQUIRE(dt.record_count() == num_records);

    ByteArray arena(1024);
    record.data = arena.data();

    for (int i = 0; i < num_records; i++) {
      dt.record(context.get(), &arena, &record, 0, i);
      REQUIRE(record.size == record_size);
	  if (record_size > 0)
        REQUIRE(0 == ::memcmp(record.data, &(model[i][0]), record_size));
    }

    // clean up
    dt.erase_record(context.get(), 0, true);
  }

  void insertEraseAscendingTest(bool fixed_records, size_t record_size) {
    DuplicateTable dt(ldb(), fixed_records && record_size <= 8,
                    record_size <= 8 ? record_size : UPS_RECORD_SIZE_UNLIMITED);

    const int num_records = 100;

    // create an empty table
    dt.create(context.get(), 0, 0);

    // the model stores the records that we inserted
    std::vector<std::vector<uint8_t> > model;

    // fill it
    ups_record_t record = {0};
    uint8_t buf[1024] = {0};
    record.data = &buf[0];
    record.size = (uint32_t)record_size;
    for (int i = 0; i < num_records; i++) {
      *(size_t *)&buf[0] = i;
      dt.set_record(context.get(), i, &record, UPS_DUPLICATE_INSERT_LAST, 0);
      model.push_back(std::vector<uint8_t>(&buf[0], &buf[record_size]));
    }

    REQUIRE(dt.record_count() == num_records);

    ByteArray arena(1024);
    record.data = arena.data();

    for (int i = 0; i < num_records; i++) {
      dt.erase_record(context.get(), 0, false);

      REQUIRE(dt.record_count() == num_records - i - 1);
      model.erase(model.begin());

      for (int j = 0; j < num_records - i - 1; j++) {
        dt.record(context.get(), &arena, &record, 0, j);
        REQUIRE(record.size == record_size);
		if (record_size > 0)
          REQUIRE(0 == memcmp(record.data, &(model[j][0]), record_size));
      }
    }

    REQUIRE(dt.record_count() == 0);
    // clean up
    dt.erase_record(context.get(), 0, true);
  }

  void insertEraseDescendingTest(bool fixed_records, size_t record_size) {
    DuplicateTable dt(ldb(), fixed_records && record_size <= 8,
                    record_size <= 8 ? record_size : UPS_RECORD_SIZE_UNLIMITED);

    const int num_records = 100;

    // create an empty table
    dt.create(context.get(), 0, 0);

    // the model stores the records that we inserted
    std::vector<std::vector<uint8_t> > model;

    // fill it
    ups_record_t record = {0};
    uint8_t buf[1024] = {0};
    record.data = &buf[0];
    record.size = (uint32_t)record_size;
    for (int i = num_records; i > 0; i--) {
      *(size_t *)&buf[0] = i;
      dt.set_record(context.get(), 0, &record, UPS_DUPLICATE_INSERT_FIRST, 0);
      model.insert(model.begin(),
                      std::vector<uint8_t>(&buf[0], &buf[record_size]));
    }

    REQUIRE(dt.record_count() == num_records);

    ByteArray arena(1024);
    record.data = arena.data();

    for (int i = num_records; i > 0; i--) {
      dt.erase_record(context.get(), i - 1, false);

      REQUIRE(dt.record_count() == i - 1);
      model.erase(model.end() - 1);

      for (int j = 0; j < i - 1; j++) {
        dt.record(context.get(), &arena, &record, 0, j);
        REQUIRE(record.size == record_size);
		if (record_size > 0)
          REQUIRE(0 == memcmp(record.data, &(model[j][0]), record_size));
      }
    }

    REQUIRE(dt.record_count() == 0);
    // clean up
    dt.erase_record(context.get(), 0, true);
  }

  void insertEraseRandomTest(bool fixed_records, size_t record_size) {
    DuplicateTable dt(ldb(), fixed_records && record_size <= 8,
                    record_size <= 8 ? record_size : UPS_RECORD_SIZE_UNLIMITED);

    const int num_records = 100;

    // create an empty table
    dt.create(context.get(), 0, 0);
    REQUIRE(dt.record_count() == 0);
    REQUIRE(dt.record_capacity() == 0);

    // the model stores the records that we inserted
    std::vector<std::vector<uint8_t> > model;

    // fill it
    ups_record_t record = {0};
    uint8_t buf[1024] = {0};
    record.data = &buf[0];
    record.size = (uint32_t)record_size;
    for (int i = 0; i < num_records; i++) {
      *(size_t *)&buf[0] = i;
      dt.set_record(context.get(), i, &record, UPS_DUPLICATE_INSERT_LAST, 0);
      model.push_back(std::vector<uint8_t>(&buf[0], &buf[record_size]));
    }

    REQUIRE(dt.record_count() == num_records);

    ByteArray arena(1024);
    record.data = arena.data();

    for (int i = 0; i < num_records; i++) {
      int position = rand() % (num_records - i);
      dt.erase_record(context.get(), position, false);

      REQUIRE(dt.record_count() == num_records - i - 1);
      model.erase(model.begin() + position);

      for (int j = 0; j < num_records - i - 1; j++) {
        dt.record(context.get(), &arena, &record, 0, j);
        REQUIRE(record.size == record_size);
		if (record_size > 0)
          REQUIRE(0 == ::memcmp(record.data, &(model[j][0]), record_size));
      }
    }

    REQUIRE(dt.record_count() == 0);
    // clean up
    dt.erase_record(context.get(), 0, true);
  }

  void insertOverwriteTest(bool fixed_records, size_t record_size) {
    DuplicateTable dt(ldb(), fixed_records && record_size <= 8,
                    record_size <= 8 ? record_size : UPS_RECORD_SIZE_UNLIMITED);

    const int num_records = 100;

    // create an empty table
    dt.create(context.get(), 0, 0);
    REQUIRE(dt.record_count() == 0);
    REQUIRE(dt.record_capacity() == 0);

    // the model stores the records that we inserted
    std::vector<std::vector<uint8_t> > model;

    // fill it
    ups_record_t record = {0};
    uint8_t buf[1024] = {0};
    record.data = &buf[0];
    record.size = (uint32_t)record_size;
    for (int i = 0; i < num_records; i++) {
      *(size_t *)&buf[0] = i;
      dt.set_record(context.get(), i, &record, UPS_DUPLICATE_INSERT_LAST, 0);
      model.push_back(std::vector<uint8_t>(&buf[0], &buf[record_size]));
    }

    REQUIRE(dt.record_count() == num_records);

    // overwrite
    for (int i = 0; i < num_records; i++) {
      *(size_t *)&buf[0] = i + 1000;
      dt.set_record(context.get(), i, &record, UPS_OVERWRITE, 0);
      model[i] = std::vector<uint8_t>(&buf[0], &buf[record_size]);
    }

    REQUIRE(dt.record_count() == num_records);

    ByteArray arena(1024);
    record.data = arena.data();

    for (int i = 0; i < num_records; i++) {
      dt.record(context.get(), &arena, &record, 0, i);
      REQUIRE(record.size == record_size);
	  if (record_size > 0)
        REQUIRE(0 == ::memcmp(record.data, &(model[i][0]), record_size));
    }
    // clean up
    dt.erase_record(context.get(), 0, true);
  }

  void insertOverwriteSizesTest() {
    DuplicateTable dt(ldb(), false, UPS_RECORD_SIZE_UNLIMITED);

    const int num_records = 1000;

    // create an empty table
    dt.create(context.get(), 0, 0);
    REQUIRE(dt.record_count() == 0);
    REQUIRE(dt.record_capacity() == 0);

    // the model stores the records that we inserted
    std::vector<std::vector<uint8_t> > model;

    // fill it
    ups_record_t record = {0};
    uint8_t buf[1024] = {0};
    record.data = &buf[0];
    for (int i = 0; i < num_records; i++) {
      *(size_t *)&buf[0] = i;
      record.size = (uint32_t)(i % 15);
      dt.set_record(context.get(), i, &record, UPS_DUPLICATE_INSERT_LAST, 0);
      model.push_back(std::vector<uint8_t>(&buf[0], &buf[record.size]));
    }

    REQUIRE(dt.record_count() == num_records);

    // overwrite
    for (int i = 0; i < num_records; i++) {
      *(size_t *)&buf[0] = i + 1000;
      record.size = (uint32_t)((i + 1) % 15);
      dt.set_record(context.get(), i, &record, UPS_OVERWRITE, 0);
      model[i] = std::vector<uint8_t>(&buf[0], &buf[record.size]);
    }

    REQUIRE(dt.record_count() == num_records);

    ByteArray arena(1024);
    for (int i = 0; i < num_records; i++) {
      record.data = arena.data();
      *(size_t *)&buf[0] = i + 1000;
      dt.record(context.get(), &arena, &record, 0, i);
      REQUIRE(record.size == (uint32_t)((i + 1) % 15));
	  if (record.size > 0)
        REQUIRE(0 == ::memcmp(record.data, &(model[i][0]), record.size));
    }
    // clean up
    dt.erase_record(context.get(), 0, true);
  }
};

TEST_CASE("BtreeDefault/DuplicateTable/createReopenTest", "")
{
  const int num_records = 100;
  uint64_t inline_data_8[num_records];
  size_t record_sizes_8[num_records];
  for (int i = 0; i < num_records; i++) {
    record_sizes_8[i] = 8;
    inline_data_8[i] = (uint64_t)i;
  }

  uint8_t default_data_0[num_records * 9] = {0};
  size_t record_sizes_0[num_records] = {0};
  for (int i = 0; i < num_records; i++)
    default_data_0[i * 9] = BtreeRecord::kBlobSizeEmpty; // flags

  uint8_t default_data_4[num_records * 9] = {0};
  size_t record_sizes_4[num_records] = {0};
  for (int i = 0; i < num_records; i++) {
    record_sizes_4[i] = 4;
    default_data_4[i * 9] = BtreeRecord::kBlobSizeTiny; // flags
    default_data_4[i * 9 + 1 + 7] = (uint8_t)4; // inline size
    *(uint32_t *)&default_data_4[i * 9 + 1] = (uint32_t)i;
  }

  uint8_t default_data_8[num_records * 9] = {0};
  for (int i = 0; i < num_records; i++) {
    default_data_8[i * 9] = BtreeRecord::kBlobSizeSmall; // flags
    *(uint64_t *)&default_data_8[i * 9 + 1] = (uint64_t)i;
  }

  uint8_t default_data_16[num_records * 9] = {0};
  size_t record_sizes_16[num_records] = {0};
  for (int i = 0; i < num_records; i++) {
    record_sizes_16[i] = 16;
  }

  uint32_t env_flags[] = {0, UPS_IN_MEMORY};
  for (int i = 0; i < 2; i++) {
    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 8, inline
      f.createReopenTest(true, 8, (uint8_t *)&inline_data_8[0],
                      record_sizes_8, num_records);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 0, inline
      f.createReopenTest(true, 0, (uint8_t *)&inline_data_8[0],
                      record_sizes_0, num_records);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 0, inline
      f.createReopenTest(false, UPS_RECORD_SIZE_UNLIMITED,
                      &default_data_0[0], record_sizes_0, num_records);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 4, inline
      f.createReopenTest(false, UPS_RECORD_SIZE_UNLIMITED,
                      &default_data_4[0], record_sizes_4, num_records);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 8, inline
      f.createReopenTest(false, UPS_RECORD_SIZE_UNLIMITED,
                      &default_data_8[0], record_sizes_8, num_records);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      Context context(f.lenv(), 0, f.ldb());

      char buffer[16] = {0};
      ups_record_t record = {0};
      record.data = &buffer[0];
      record.size = 16;
      for (int i = 0; i < num_records; i++) {
        uint64_t blob_id = f.lenv()->blob_manager->allocate(&context,
                        &record, 0);
        context.changeset.clear(); // unlock pages
        *(uint64_t *)&default_data_16[i * 9 + 1] = blob_id;
      }

      // variable length records of size 16, not inline
      f.createReopenTest(false, UPS_RECORD_SIZE_UNLIMITED,
                      &default_data_16[0], record_sizes_16, num_records);

      // no need to clean up allocated blobs; they will be erased when
      // the DuplicateTable goes out of scope
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      Context context(f.lenv(), 0, f.ldb());

      char buffer[16] = {0};
      ups_record_t record = {0};
      record.data = &buffer[0];
      record.size = 16;
      for (int i = 0; i < num_records; i++) {
        uint64_t blob_id = f.lenv()->blob_manager->allocate(&context,
                        &record, 0);
        context.changeset.clear(); // unlock pages
        *(uint64_t *)&default_data_16[i * 9 + 1] = blob_id;
      }

      // fixed length records of size 16, not inline
      f.createReopenTest(false, 16,
                      &default_data_16[0], record_sizes_16, num_records);

      // no need to clean up allocated blobs; they will be erased when
      // the DuplicateTable goes out of scope
    }
  }
}

TEST_CASE("BtreeDefault/DuplicateTable/insertAscendingTest", "")
{
  uint32_t env_flags[] = {0, UPS_IN_MEMORY};
  for (int i = 0; i < 2; i++) {
    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 8, inline
      f.insertAscendingTest(true, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 0, inline
      f.insertAscendingTest(true, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 0
      f.insertAscendingTest(false, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 4
      f.insertAscendingTest(false, 4);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 8
      f.insertAscendingTest(false, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 16, not inline
      f.insertAscendingTest(false, 16);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 16, not inline
      f.insertAscendingTest(true, 16);
    }
  }
}

TEST_CASE("BtreeDefault/DuplicateTable/insertDescendingTest", "")
{
  uint32_t env_flags[] = {0, UPS_IN_MEMORY};
  for (int i = 0; i < 2; i++) {
    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 8, inline
      f.insertDescendingTest(true, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 0, inline
      f.insertDescendingTest(true, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 0
      f.insertDescendingTest(false, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 4
      f.insertDescendingTest(false, 4);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 8
      f.insertDescendingTest(false, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 16, not inline
      f.insertDescendingTest(false, 16);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 16, not inline
      f.insertDescendingTest(true, 16);
    }
  }
}

TEST_CASE("BtreeDefault/DuplicateTable/insertRandomTest", "")
{
  uint32_t env_flags[] = {0, UPS_IN_MEMORY};
  for (int i = 0; i < 2; i++) {
    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 8, inline
      f.insertRandomTest(true, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 0, inline
      f.insertRandomTest(true, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 0
      f.insertRandomTest(false, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 4
      f.insertRandomTest(false, 4);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 8
      f.insertRandomTest(false, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 16, not inline
      f.insertRandomTest(false, 16);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 16, not inline
      f.insertRandomTest(true, 16);
    }
  }
}

TEST_CASE("BtreeDefault/DuplicateTable/insertEraseAscendingTest", "")
{
  uint32_t env_flags[] = {0, UPS_IN_MEMORY};
  for (int i = 0; i < 2; i++) {
    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 8, inline
      f.insertEraseAscendingTest(true, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 0, inline
      f.insertEraseAscendingTest(true, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 0
      f.insertEraseAscendingTest(false, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 4
      f.insertEraseAscendingTest(false, 4);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 8
      f.insertEraseAscendingTest(false, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 16, not inline
      f.insertEraseAscendingTest(false, 16);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 16, not inline
      f.insertEraseAscendingTest(true, 16);
    }
  }
}

TEST_CASE("BtreeDefault/DuplicateTable/insertEraseDescendingTest", "")
{
  uint32_t env_flags[] = {0, UPS_IN_MEMORY};
  for (int i = 0; i < 2; i++) {
    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 8, inline
      f.insertEraseDescendingTest(true, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 0, inline
      f.insertEraseDescendingTest(true, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 0
      f.insertEraseDescendingTest(false, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 4
      f.insertEraseDescendingTest(false, 4);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 8
      f.insertEraseDescendingTest(false, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 16, not inline
      f.insertEraseDescendingTest(false, 16);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 16, not inline
      f.insertEraseDescendingTest(true, 16);
    }
  }
}

TEST_CASE("BtreeDefault/DuplicateTable/insertEraseRandomTest", "")
{
  uint32_t env_flags[] = {0, UPS_IN_MEMORY};
  for (int i = 0; i < 2; i++) {
    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 8, inline
      f.insertEraseRandomTest(true, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 0, inline
      f.insertEraseRandomTest(true, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 0
      f.insertEraseRandomTest(false, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 4
      f.insertEraseRandomTest(false, 4);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 8
      f.insertEraseRandomTest(false, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 16, not inline
      f.insertEraseRandomTest(false, 16);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 16, not inline
      f.insertEraseRandomTest(true, 16);
    }
  }
}

TEST_CASE("BtreeDefault/DuplicateTable/insertOverwriteTest", "")
{
  uint32_t env_flags[] = {0, UPS_IN_MEMORY};
  for (int i = 0; i < 2; i++) {
    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 8, inline
      f.insertOverwriteTest(true, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 0, inline
      f.insertOverwriteTest(true, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 0
      f.insertOverwriteTest(false, 0);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 4
      f.insertOverwriteTest(false, 4);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 8
      f.insertOverwriteTest(false, 8);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // variable length records of size 16, not inline
      f.insertOverwriteTest(false, 16);
    }

    {
      DuplicateTableFixture f(env_flags[i]);
      // fixed length records of size 16, not inline
      f.insertOverwriteTest(true, 16);
    }
  }
}

TEST_CASE("BtreeDefault/DuplicateTable/insertOverwriteSizesTest", "")
{
  uint32_t env_flags[] = {0, UPS_IN_MEMORY};
  for (int i = 0; i < 2; i++) {
    DuplicateTableFixture f(env_flags[i]);
    f.insertOverwriteSizesTest();
  }
}

struct UpfrontIndexFixture : BaseFixture {
  ScopedPtr<Context> context;

  UpfrontIndexFixture(size_t page_size) {
    ups_parameter_t params[] = {
        {UPS_PARAM_PAGE_SIZE, page_size},
        {0, 0}
    };

    require_create(0, params, UPS_ENABLE_DUPLICATES, nullptr);
    context.reset(new Context(lenv(), 0, ldb()));
  }

  ~UpfrontIndexFixture() {
    teardown();
  }

  void teardown() {
    context->changeset.clear();
    close();
  }

  void createReopenTest() {
    uint8_t data[1024 * 16] = {1};

    UpfrontIndex ui(ldb());
    REQUIRE(ui.full_index_size() == 3);
    ui.create(&data[0], sizeof(data), 300);

    REQUIRE(ui.freelist_count() == 0);
    REQUIRE(ui.capacity() == 300);
    REQUIRE(ui.next_offset(0) == 0);

    UpfrontIndex ui2(ldb());
    REQUIRE(ui2.full_index_size() == 3);
    ui2.open(&data[0], 300);
    REQUIRE(ui2.freelist_count() == 0);
    REQUIRE(ui2.capacity() == 300);
    REQUIRE(ui2.next_offset(0) == 0);
  }

  void appendSlotTest() {
    uint8_t data[1024 * 16] = {1};

    UpfrontIndex ui(ldb());
    REQUIRE(ui.full_index_size() == 3);
    ui.create(&data[0], sizeof(data), 300);

    for (size_t i = 0; i < 300; i++) {
      REQUIRE(ui.can_insert(i) == true);
      ui.insert(i, i); // position, count
    }
    REQUIRE(ui.can_insert(300) == false);
  }

  void insertSlotTest() {
    uint8_t data[1024 * 16] = {1};
    const size_t kMax = 300;

    UpfrontIndex ui(ldb());
    REQUIRE(ui.full_index_size() == 3);
    ui.create(&data[0], sizeof(data), kMax);

    for (size_t i = 0; i < kMax; i++) {
      REQUIRE(ui.can_insert(i) == true);
      ui.insert(0, i); // position, count
    }
    REQUIRE(ui.can_insert(kMax) == false);
  }

  void eraseSlotTest() {
    uint8_t data[1024 * 16] = {1};
    const size_t kMax = 200;

    UpfrontIndex ui(ldb());
    REQUIRE(ui.full_index_size() == 3);
    ui.create(&data[0], sizeof(data), kMax);

    for (size_t i = 0; i < kMax; i++) {
      REQUIRE(ui.can_insert(i) == true);
      ui.insert(i, i); // position, count
      ui.set_chunk_size(i, i);
      ui.set_chunk_offset(i, i);
    }
    REQUIRE(ui.can_insert(kMax) == false);

    for (size_t i = 0; i < kMax - 1; i++) {
      ui.erase(kMax - i, 0);
      REQUIRE(ui.freelist_count() == i + 1);
      REQUIRE(ui.get_chunk_size(0) == i + 1);
      REQUIRE(ui.get_chunk_offset(0) == i + 1);
    }

    ui.create(&data[0], sizeof(data), kMax);

    // fill again, then erase from behind
    for (size_t i = 0; i < kMax; i++) {
      REQUIRE(ui.can_insert(i) == true);
      ui.insert(i, i); // position, count
      ui.set_chunk_size(i, i);
      ui.set_chunk_offset(i, i);
    }
    REQUIRE(ui.can_insert(kMax) == false);

    for (size_t i = 0; i < kMax; i++) {
      ui.erase(kMax - i, kMax - 1 - i);
      REQUIRE(ui.freelist_count() == i + 1);
      for (size_t j = 0; j < kMax; j++) { // also checks freelist
        REQUIRE(ui.get_chunk_size(j) == j);
        REQUIRE(ui.get_chunk_offset(j) == j);
      }
    }
  }

  void allocateTest() {
    uint8_t data[1024 * 16] = {1};
    const size_t kMax = 300;

    UpfrontIndex ui(ldb());
    ui.create(&data[0], sizeof(data), kMax);

    size_t bytes_left = sizeof(data) - kMax * ui.full_index_size()
            - UpfrontIndex::kPayloadOffset;

    size_t i;
    size_t capacity = bytes_left / 64;
    for (i = 0; i < capacity; i++) {
      REQUIRE(ui.can_allocate_space(i, 64) == true);
      REQUIRE(ui.allocate_space(i, i, 64) == i * 64); // count, slot, size
    }
    REQUIRE(ui.can_allocate_space(i, 64) == false);
  }

  void allocateFromFreelistTest() {
    uint8_t data[1024 * 16] = {1};
    const size_t kMax = 300;

    UpfrontIndex ui(ldb());
    ui.create(&data[0], sizeof(data), kMax);

    size_t bytes_left = sizeof(data) - kMax * ui.full_index_size()
            - UpfrontIndex::kPayloadOffset;

    // fill it up
    size_t i;
    size_t capacity = bytes_left / 64;
    for (i = 0; i < capacity; i++) {
      REQUIRE(ui.can_allocate_space(i, 64) == true);
      REQUIRE(ui.allocate_space(i, i, 64) == i * 64); // count, slot, size
    }
    REQUIRE(ui.can_allocate_space(i, 64) == false);

    // erase the last slot, allocate it again
    REQUIRE(ui.freelist_count() == 0);
    ui.erase(i, i - 1);
    REQUIRE(ui.freelist_count() == 1);
    REQUIRE(ui.can_allocate_space(i - 1, 64) == true);
    REQUIRE(ui.allocate_space(i - 1, i - 1, 64) > 0);
    REQUIRE(ui.can_allocate_space(i, 64) == false);

    // erase the first slot, allocate it again
    REQUIRE(ui.freelist_count() == 0);
    ui.erase(i, 0);
    REQUIRE(ui.freelist_count() == 1);
    REQUIRE(ui.can_allocate_space(i - 1, 64) == true);
    REQUIRE(ui.allocate_space(i - 1, i - 1, 64) == 0);
    REQUIRE(ui.can_allocate_space(i, 64) == false);
  }

  void splitMergeTest() {
    uint8_t data1[1024 * 16] = {1};
    uint8_t data2[1024 * 16] = {1};
    const size_t kMax = 300;

    UpfrontIndex ui1(ldb());
    ui1.create(&data1[0], sizeof(data1), kMax);

    size_t bytes_left = sizeof(data1) - kMax * ui1.full_index_size()
            - UpfrontIndex::kPayloadOffset;

    // fill it up
    size_t capacity = bytes_left / 64;
    for (size_t i = 0; i < capacity; i++) {
      REQUIRE(ui1.allocate_space(i, i, 64) == i * 64); // count, slot, size
      ui1.set_chunk_size(i, 64);
      ui1.set_chunk_offset(i, i * 64);
    }

    // at every possible position: split into page2, then merge, then compare
    for (size_t i = 0; i < capacity; i++) {
      UpfrontIndex ui2(ldb());
      ui2.create(&data2[0], sizeof(data2), kMax);
      ui1.split(&ui2, capacity, i);
      ui1.merge_from(&ui2, capacity - i, i);

      for (size_t j = 0; j < capacity; j++) {
        REQUIRE(ui1.get_chunk_size(j) == 64);
        REQUIRE(ui1.get_chunk_offset(j) == j * 64);
      }
    }
  }
};

TEST_CASE("BtreeDefault/UpfrontIndex/createReopenTest", "")
{
  size_t page_sizes[] = {1024 * 16, 1024 * 64};
  for (int i = 0; i < 2; i++) {
    UpfrontIndexFixture f(page_sizes[i]);
    f.createReopenTest();
  }
}

TEST_CASE("BtreeDefault/UpfrontIndex/appendSlotTest", "")
{
  size_t page_sizes[] = {1024 * 16, 1024 * 64};
  for (int i = 0; i < 2; i++) {
    UpfrontIndexFixture f(page_sizes[i]);
    f.appendSlotTest();
  }
}

TEST_CASE("BtreeDefault/UpfrontIndex/insertSlotTest", "")
{
  size_t page_sizes[] = {1024 * 16, 1024 * 64};
  for (int i = 0; i < 2; i++) {
    UpfrontIndexFixture f(page_sizes[i]);
    f.insertSlotTest();
  }
}

TEST_CASE("BtreeDefault/UpfrontIndex/eraseSlotTest", "")
{
  size_t page_sizes[] = {1024 * 16, 1024 * 64};
  for (int i = 0; i < 2; i++) {
    UpfrontIndexFixture f(page_sizes[i]);
    f.eraseSlotTest();
  }
}

TEST_CASE("BtreeDefault/UpfrontIndex/allocateTest", "")
{
  size_t page_sizes[] = {1024 * 16, 1024 * 64};
  for (int i = 0; i < 2; i++) {
    UpfrontIndexFixture f(page_sizes[i]);
    f.allocateTest();
  }
}

TEST_CASE("BtreeDefault/UpfrontIndex/allocateFromFreelistTest", "")
{
  size_t page_sizes[] = {1024 * 16, 1024 * 64};
  for (int i = 0; i < 2; i++) {
    UpfrontIndexFixture f(page_sizes[i]);
    f.allocateFromFreelistTest();
  }
}

TEST_CASE("BtreeDefault/UpfrontIndex/splitMergeTest", "")
{
  size_t page_sizes[] = {1024 * 16, 1024 * 64};
  for (int i = 0; i < 2; i++) {
    UpfrontIndexFixture f(page_sizes[i]);
    f.splitMergeTest();
  }
}

} // namespace upscaledb
