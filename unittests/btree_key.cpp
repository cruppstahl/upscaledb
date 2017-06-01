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

#include "3btree/btree_index.h"
#include "3btree/btree_flags.h"
#include "3btree/btree_node_proxy.h"
#include "3btree/btree_node.h"
#include "4context/context.h"

#include "os.hpp"
#include "fixture.hpp"

namespace upscaledb {

struct BtreeKeyFixture : BaseFixture {
  ScopedPtr<Context> context;
  Page *page;

  BtreeKeyFixture(bool duplicate = false) {
    uint32_t flags = 0;
    if (duplicate)
      flags |= UPS_ENABLE_DUPLICATE_KEYS;

    require_create(0, 0, flags, 0);

    context.reset(new Context(lenv(), 0, ldb()));

    page = page_manager()->alloc(context.get(), Page::kTypeBindex,
                    PageManager::kClearWithZero);

    // this is a leaf page! internal pages cause different behavior... 
    PBtreeNode *node = PBtreeNode::from_page(page);
    node->set_flags(PBtreeNode::kLeafNode);
  }

  ~BtreeKeyFixture() {
    context->changeset.clear();
  }

  void insertEmpty(uint32_t flags) {
    BtreeNodeProxyProxy bnpp(btree_index(), page);

    ups_key_t key = {0};
    ups_record_t record = {0};

    PBtreeNode::InsertResult result(0, 0);
    if (!flags)
      result = bnpp.node->insert(context.get(), &key, 0);

    bnpp.node->set_record(context.get(), result.slot, &record, 0, flags, 0);
  }

  void prepareEmpty() {
    insertEmpty(0);
  }

  void overwriteEmpty() {
    insertEmpty(UPS_OVERWRITE);
  }

  void duplicateEmpty() {
    insertEmpty(UPS_DUPLICATE);
  }

  void insertTiny(const char *data, uint32_t size, uint32_t flags) {
    BtreeNodeProxyProxy bnpp(btree_index(), page);
    ByteArray arena;

    ups_key_t key = {0};

    PBtreeNode::InsertResult result(0, 0);
    if (!flags)
      result = bnpp.node->insert(context.get(), &key, 0);

    ups_record_t record = ups_make_record((void *)data, size);

    bnpp.node->set_record(context.get(), result.slot, &record, 0, flags, 0);

    if (NOTSET(flags, UPS_DUPLICATE)) {
      ups_record_t record2 = {0};
      bnpp.node->record(context.get(), result.slot, &arena, &record2, 0);
      REQUIRE(record.size == record2.size);
      REQUIRE(0 == ::memcmp(record.data, record2.data, record.size));
    }
    else
      REQUIRE(bnpp.node->record_count(context.get(), result.slot) > 1);
  }

  void prepareTiny(const char *data, uint32_t size) {
    insertTiny(data, size, 0);
  }

  void overwriteTiny(const char *data, uint32_t size) {
    insertTiny(data, size, UPS_OVERWRITE);
  }

  void duplicateTiny(const char *data, uint32_t size) {
    insertTiny(data, size, UPS_DUPLICATE);
  }

  void insertSmall(const char *data, uint32_t flags) {
    BtreeNodeProxyProxy bnpp(btree_index(), page);
    ByteArray arena;

    ups_key_t key = {0};
    PBtreeNode::InsertResult result(0, 0);
    if (!flags)
      result = bnpp.node->insert(context.get(), &key, 0);

    ups_record_t record = ups_make_record((void *)data, sizeof(uint64_t));

    bnpp.node->set_record(context.get(), result.slot, &record, 0, flags, 0);
    if (ISSET(flags, UPS_DUPLICATE))
      REQUIRE(bnpp.node->record_count(context.get(), result.slot) > 1);

    if (NOTSET(flags, UPS_DUPLICATE)) {
      ups_record_t record2 = {0};
      bnpp.node->record(context.get(), result.slot, &arena, &record2, 0);
      REQUIRE(record.size == record2.size);
      REQUIRE(0 == ::memcmp(record.data, record2.data, record.size));
    }
  }

  void prepareSmall(const char *data) {
    insertSmall(data, 0);
  }

  void overwriteSmall(const char *data) {
    insertSmall(data, UPS_OVERWRITE);
  }

  void duplicateSmall(const char *data) {
    insertSmall(data, UPS_DUPLICATE);
  }

  void insertNormal(const char *data, uint32_t size, uint32_t flags) {
    BtreeNodeProxyProxy bnpp(btree_index(), page);
    ByteArray arena;
    ups_key_t key = {0};

    PBtreeNode::InsertResult result(0, 0);
    if (!flags)
      result = bnpp.node->insert(context.get(), &key, 0);

    ups_record_t record = ups_make_record((void *)data, size);

    bnpp.node->set_record(context.get(), result.slot, &record, 0, flags, 0);
    if (ISSET(flags, UPS_DUPLICATE))
      REQUIRE(bnpp.node->record_count(context.get(), result.slot) > 1);

    if (NOTSET(flags, UPS_DUPLICATE)) {
      ups_record_t record2 = {0};
      bnpp.node->record(context.get(), result.slot, &arena, &record2, 0);
      REQUIRE(record.size == record2.size);
      REQUIRE(0 == ::memcmp(record.data, record2.data, record.size));
    }
  }

  void prepareNormal(const char *data, uint32_t size) {
    insertNormal(data, size, 0);
  }

  void overwriteNormal(const char *data, uint32_t size) {
    insertNormal(data, size, UPS_OVERWRITE);
  }

  void duplicateNormal(const char *data, uint32_t size) {
    insertNormal(data, size, UPS_DUPLICATE);
  }

  void resetPage() {
    page_manager()->del(context.get(), page);

    page = page_manager()->alloc(context.get(), Page::kTypeBindex,
                    PageManager::kClearWithZero);
    PBtreeNode *node = PBtreeNode::from_page(page);
    node->set_flags(PBtreeNode::kLeafNode);
  }

  void setRecordTest() {
    // set empty record
    prepareEmpty();

    // set tiny record
    prepareTiny("1234", 4);

    // set small record
    prepareSmall("12345678");

    // set normal record
    prepareNormal("1234567812345678", sizeof(uint64_t)*2);
  }

  void overwriteRecordTest() {
    // overwrite empty record with a tiny key
    prepareEmpty();
    overwriteTiny("1234", 4);

    // overwrite empty record with an empty key
    prepareEmpty();
    overwriteEmpty();

    // overwrite empty record with a normal key
    prepareEmpty();
    overwriteNormal("1234123456785678", 16);

    // overwrite tiny record with an empty key
    prepareTiny("1234", 4);
    overwriteEmpty();

    // overwrite tiny record with a normal key
    prepareTiny("1234", 4);
    overwriteNormal("1234123456785678", 16);

    // overwrite small record with an empty key
    prepareSmall("12341234");
    overwriteEmpty();

    // overwrite small record with a normal key
    prepareSmall("12341234");
    overwriteNormal("1234123456785678", 16);

    // overwrite normal record with an empty key
    prepareNormal("1234123456785678", 16);
    overwriteEmpty();

    // overwrite normal record with a small key
    prepareNormal("1234123456785678", 16);
    overwriteSmall("12341234");

    // overwrite normal record with a tiny key
    prepareNormal("1234123456785678", 16);
    overwriteTiny("1234", 4);

    // overwrite normal record with a normal key
    prepareNormal("1234123456785678", 16);
    overwriteNormal("1234123456785678", 16);
  }

  void checkDupe(int position, const char *data, uint32_t size) {
    BtreeNodeProxyProxy bnpp(btree_index(), page);
    int slot = 0;
    REQUIRE(bnpp.node->record_count(context.get(), slot) >= 1);

    ups_record_t record = {0};

    ByteArray arena;
    bnpp.node->record(context.get(), slot, &arena, &record, 0, position);
    REQUIRE(record.size == size);
    if (size)
      REQUIRE(0 == ::memcmp(record.data, data, record.size));
    else
      REQUIRE(record.data == nullptr);
  }

  void duplicateRecordTest() {
    // insert empty key, then another empty duplicate
    prepareEmpty();
    duplicateEmpty();
    checkDupe(0, 0, 0);
    checkDupe(1, 0, 0);

    // insert empty key, then another small duplicate
    resetPage();
    prepareEmpty();
    duplicateSmall("12345678");
    checkDupe(0, 0, 0);
    checkDupe(1, "12345678", 8);

    // insert empty key, then another tiny duplicate
    resetPage();
    prepareEmpty();
    duplicateTiny("1234", 4);
    checkDupe(0, 0, 0);
    checkDupe(1, "1234", 4);

    // insert empty key, then another normal duplicate
    resetPage();
    prepareEmpty();
    duplicateNormal("1234567812345678", 16);
    checkDupe(0, 0, 0);
    checkDupe(1, "1234567812345678", 16);

    // insert tiny key, then another empty duplicate
    resetPage();
    prepareTiny("1234", 4);
    duplicateEmpty();
    checkDupe(0, "1234", 4);
    checkDupe(1, 0, 0);

    // insert tiny key, then another small duplicate
    resetPage();
    prepareTiny("1234", 4);
    duplicateSmall("12345678");
    checkDupe(0, "1234", 4);
    checkDupe(1, "12345678", 8);

    // insert tiny key, then another tiny duplicate
    resetPage();
    prepareTiny("1234", 4);
    duplicateTiny("23456", 5);
    checkDupe(0, "1234", 4);
    checkDupe(1, "23456", 5);

    // insert tiny key, then another normal duplicate
    resetPage();
    prepareTiny("1234", 4);
    duplicateNormal("1234567812345678", 16);
    checkDupe(0, "1234", 4);
    checkDupe(1, "1234567812345678", 16);

    // insert small key, then another empty duplicate
    resetPage();
    prepareSmall("12341234");
    duplicateEmpty();
    checkDupe(0, "12341234", 8);
    checkDupe(1, 0, 0);

    // insert small key, then another small duplicate
    resetPage();
    prepareSmall("xx341234");
    duplicateSmall("12345678");
    checkDupe(0, "xx341234", 8);
    checkDupe(1, "12345678", 8);

    // insert small key, then another tiny duplicate
    resetPage();
    prepareSmall("12341234");
    duplicateTiny("1234", 4);
    checkDupe(0, "12341234", 8);
    checkDupe(1, "1234", 4);

    // insert small key, then another normal duplicate
    resetPage();
    prepareSmall("12341234");
    duplicateNormal("1234567812345678", 16);
    checkDupe(0, "12341234", 8);
    checkDupe(1, "1234567812345678", 16);

    // insert normal key, then another empty duplicate
    resetPage();
    prepareNormal("1234123456785678", 16);
    duplicateEmpty();
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, 0, 0);

    // insert normal key, then another small duplicate
    resetPage();
    prepareNormal("1234123456785678", 16);
    duplicateSmall("12345678");
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "12345678", 8);

    // insert normal key, then another tiny duplicate
    resetPage();
    prepareNormal("1234123456785678", 16);
    duplicateTiny("1234", 4);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "1234", 4);

    // insert normal key, then another normal duplicate
    resetPage();
    prepareNormal("1234123456785678", 16);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "abc4567812345678", 16);
  }

  void eraseRecordTest() {
    BtreeNodeProxyProxy bnpp(btree_index(), page);

    // insert empty key, then delete it
    prepareEmpty();
    bnpp.node->erase_record(context.get(), 0, 0, false, 0);
    REQUIRE(bnpp.node->record_id(context.get(), 0) == 0);

    // insert tiny key, then delete it
    prepareTiny("1234", 4);
    bnpp.node->erase_record(context.get(), 0, 0, false, 0);
    REQUIRE(bnpp.node->record_id(context.get(), 0) == 0);

    // insert small key, then delete it
    prepareSmall("12345678");
    bnpp.node->erase_record(context.get(), 0, 0, false, 0);
    REQUIRE(bnpp.node->record_id(context.get(), 0) == 0);

    // insert normal key, then delete it
    prepareNormal("1234123456785678", 16);
    bnpp.node->erase_record(context.get(), 0, 0, false, 0);
    REQUIRE(bnpp.node->record_id(context.get(), 0) == 0);
  }

  void eraseDuplicateRecordTest1() {
    BtreeNodeProxyProxy bnpp(btree_index(), page);

    // insert empty key, then a duplicate; delete both
    prepareEmpty();
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, 0, 0);
    checkDupe(1, "abc4567812345678", 16);
    bnpp.node->erase_record(context.get(), 0, 0, true, 0);
  }

  void eraseDuplicateRecordTest2() {
    BtreeNodeProxyProxy bnpp(btree_index(), page);

    // insert tiny key, then a duplicate; delete both
    prepareTiny("1234", 4);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234", 4);
    checkDupe(1, "abc4567812345678", 16);
    bnpp.node->erase_record(context.get(), 0, 0, true, 0);
  }

  void eraseDuplicateRecordTest3() {
    BtreeNodeProxyProxy bnpp(btree_index(), page);

    // insert small key, then a duplicate; delete both
    prepareSmall("12345678");
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "12345678", 8);
    checkDupe(1, "abc4567812345678", 16);
    bnpp.node->erase_record(context.get(), 0, 0, true, 0);
  }

  void eraseDuplicateRecordTest4() {
    BtreeNodeProxyProxy bnpp(btree_index(), page);

    // insert normal key, then a duplicate; delete both
    prepareNormal("1234123456785678", 16);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "abc4567812345678", 16);
    bnpp.node->erase_record(context.get(), 0, 0, true, 0);
  }

  void eraseAllDuplicateRecordTest1() {
    BtreeNodeProxyProxy bnpp(btree_index(), page);

    // insert empty key, then a duplicate; delete both at once
    prepareEmpty();
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, 0, 0);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(bnpp.node->record_count(context.get(), 0) == 2);
    bnpp.node->erase_record(context.get(), 0, 0, false, 0);
    REQUIRE(bnpp.node->record_count(context.get(), 0) == 1);
    checkDupe(0, "abc4567812345678", 16);
    bnpp.node->erase_record(context.get(), 0, 0, false, 0);
  }

  void eraseAllDuplicateRecordTest2() {
    BtreeNodeProxyProxy bnpp(btree_index(), page);

    // insert tiny key, then a duplicate; delete both
    prepareTiny("1234", 4);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234", 4);
    checkDupe(1, "abc4567812345678", 16);
    bnpp.node->erase_record(context.get(), 0, 1, false, 0);
    REQUIRE(bnpp.node->record_count(context.get(), 0) == 1);
    checkDupe(0, "1234", 4);
    bnpp.node->erase_record(context.get(), 0, 0, false, 0);
  }

  void eraseAllDuplicateRecordTest3() {
    BtreeNodeProxyProxy bnpp(btree_index(), page);

    // insert small key, then a duplicate; delete both at once
    prepareSmall("12345678");
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "12345678", 8);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(bnpp.node->record_count(context.get(), 0) == 2);
    bnpp.node->erase_record(context.get(), 0, 0, false, 0);
    REQUIRE(bnpp.node->record_count(context.get(), 0) == 1);
    checkDupe(0, "abc4567812345678", 16);
    bnpp.node->erase_record(context.get(), 0, 0, false, 0);
  }

  void eraseAllDuplicateRecordTest4() {
    BtreeNodeProxyProxy bnpp(btree_index(), page);

    // insert normal key, then a duplicate; delete both at once
    prepareNormal("1234123456785678", 16);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(bnpp.node->record_count(context.get(), 0) == 2);
    bnpp.node->erase_record(context.get(), 0, 1, false, 0);
    REQUIRE(bnpp.node->record_count(context.get(), 0) == 1);
    checkDupe(0, "1234123456785678", 16);
    bnpp.node->erase_record(context.get(), 0, 0, false, 0);
  }
};

TEST_CASE("BtreeKey/setRecord", "")
{
  BtreeKeyFixture f;
  f.setRecordTest();
}

TEST_CASE("BtreeKey/overwriteRecord", "")
{
  BtreeKeyFixture f;
  f.overwriteRecordTest();
}

TEST_CASE("BtreeKey/duplicateRecord", "")
{
  BtreeKeyFixture f(true);
  f.duplicateRecordTest();
}

TEST_CASE("BtreeKey/eraseRecord", "")
{
  BtreeKeyFixture f;
  f.eraseRecordTest();
}

TEST_CASE("BtreeKey/eraseDuplicateRecord1", "")
{
  BtreeKeyFixture f(true);
  f.eraseDuplicateRecordTest1();
}

TEST_CASE("BtreeKey/eraseDuplicateRecord2", "")
{
  BtreeKeyFixture f(true);
  f.eraseDuplicateRecordTest2();
}

TEST_CASE("BtreeKey/eraseDuplicateRecord3", "")
{
  BtreeKeyFixture f(true);
  f.eraseDuplicateRecordTest3();
}

TEST_CASE("BtreeKey/eraseDuplicateRecord4", "")
{
  BtreeKeyFixture f(true);
  f.eraseDuplicateRecordTest4();
}

TEST_CASE("BtreeKey/eraseAllDuplicateRecord1", "")
{
  BtreeKeyFixture f(true);
  f.eraseAllDuplicateRecordTest1();
}

TEST_CASE("BtreeKey/eraseAllDuplicateRecord2", "")
{
  BtreeKeyFixture f(true);
  f.eraseAllDuplicateRecordTest2();
}

TEST_CASE("BtreeKey/eraseAllDuplicateRecord3", "")
{
  BtreeKeyFixture f(true);
  f.eraseAllDuplicateRecordTest3();
}

TEST_CASE("BtreeKey/eraseAllDuplicateRecord4", "")
{
  BtreeKeyFixture f(true);
  f.eraseAllDuplicateRecordTest4();
}

} // namespace upscaledb
