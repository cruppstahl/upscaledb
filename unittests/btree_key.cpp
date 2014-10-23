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

#include "1base/byte_array.h"
#include "2page/page.h"
#include "3btree/btree_index.h"
#include "3btree/btree_flags.h"
#include "3btree/btree_node_proxy.h"
#include "3btree/btree_impl_default.h"
#include "3page_manager/page_manager.h"
#include "3btree/btree_node.h"
#include "4db/db.h"
#include "4env/env_local.h"

namespace hamsterdb {

struct BtreeKeyFixture {
  ham_db_t *m_db;
  LocalDatabase *m_dbp;
  ham_env_t *m_env;
  Page *m_page;

  BtreeKeyFixture(bool duplicate = false) {
    os::unlink(Utils::opath(".test"));

    uint32_t flags = 0;
    if (duplicate)
      flags |= HAM_ENABLE_DUPLICATE_KEYS;
    REQUIRE(0 == ham_env_create(&m_env, Utils::opath(".test"), 0, 0644, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, flags, 0));

    m_dbp = (LocalDatabase *)m_db;

    m_page = m_dbp->get_local_env()->get_page_manager()->alloc_page(m_dbp,
                    Page::kTypeBindex,
                    PageManager::kClearWithZero);

    // this is a leaf page! internal pages cause different behavior... 
    PBtreeNode *node = PBtreeNode::from_page(m_page);
    node->set_flags(PBtreeNode::kLeafNode);
  }

  ~BtreeKeyFixture() {
    if (m_env)
	  REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void insertEmpty(uint32_t flags) {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);
    ham_key_t key = {0};
    ham_record_t rec = {0};

    PBtreeNode::InsertResult result = {0, 0};
    if (!flags)
      result = node->insert(&key, 0);

    node->set_record(result.slot, &rec, 0, flags, 0);
  }

  void prepareEmpty() {
    insertEmpty(0);
  }

  void overwriteEmpty() {
    insertEmpty(HAM_OVERWRITE);
  }

  void duplicateEmpty() {
    insertEmpty(HAM_DUPLICATE);
  }

  void insertTiny(const char *data, uint32_t size, uint32_t flags) {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);
    ByteArray arena;
    ham_record_t rec, rec2;
    ham_key_t key = {0};

    PBtreeNode::InsertResult result = {0, 0};
    if (!flags)
      result = node->insert(&key, 0);

    memset(&rec, 0, sizeof(rec));
    memset(&rec2, 0, sizeof(rec2));
    rec.data = (void *)data;
    rec.size = size;

    node->set_record(result.slot, &rec, 0, flags, 0);

    if (!(flags & HAM_DUPLICATE)) {
      node->get_record(result.slot, &arena, &rec2, 0);
      REQUIRE(rec.size == rec2.size);
      REQUIRE(0 == memcmp(rec.data, rec2.data, rec.size));
    }
    else
      REQUIRE(node->get_record_count(result.slot) > 1);
  }

  void prepareTiny(const char *data, uint32_t size) {
    insertTiny(data, size, 0);
  }

  void overwriteTiny(const char *data, uint32_t size) {
    insertTiny(data, size, HAM_OVERWRITE);
  }

  void duplicateTiny(const char *data, uint32_t size) {
    insertTiny(data, size, HAM_DUPLICATE);
  }

  void insertSmall(const char *data, uint32_t flags) {
    ByteArray arena;
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);
    ham_record_t rec, rec2;
    ham_key_t key = {0};

    PBtreeNode::InsertResult result = {0, 0};
    if (!flags)
      result = node->insert(&key, 0);

    memset(&rec, 0, sizeof(rec));
    memset(&rec2, 0, sizeof(rec2));
    rec.data = (void *)data;
    rec.size = sizeof(uint64_t);

    node->set_record(result.slot, &rec, 0, flags, 0);
    if (flags & HAM_DUPLICATE) {
      REQUIRE(node->get_record_count(result.slot) > 1);
    }

    if (!(flags & HAM_DUPLICATE)) {
      node->get_record(result.slot, &arena, &rec2, 0);
      REQUIRE(rec.size == rec2.size);
      REQUIRE(0 == memcmp(rec.data, rec2.data, rec.size));
    }
  }

  void prepareSmall(const char *data) {
    insertSmall(data, 0);
  }

  void overwriteSmall(const char *data) {
    insertSmall(data, HAM_OVERWRITE);
  }

  void duplicateSmall(const char *data) {
    insertSmall(data, HAM_DUPLICATE);
  }

  void insertNormal(const char *data, uint32_t size, uint32_t flags) {
    ByteArray arena;
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);
    ham_record_t rec, rec2;
    ham_key_t key = {0};

    PBtreeNode::InsertResult result = {0, 0};
    if (!flags)
      result = node->insert(&key, 0);

    memset(&rec, 0, sizeof(rec));
    memset(&rec2, 0, sizeof(rec2));
    rec.data = (void *)data;
    rec.size = size;

    node->set_record(result.slot, &rec, 0, flags, 0);
    if (flags & HAM_DUPLICATE)
      REQUIRE(node->get_record_count(result.slot) > 1);

    if (!(flags & HAM_DUPLICATE)) {
      node->get_record(result.slot, &arena, &rec2, 0);
      REQUIRE(rec.size == rec2.size);
      REQUIRE(0 == memcmp(rec.data, rec2.data, rec.size));
    }
  }

  void prepareNormal(const char *data, uint32_t size) {
    insertNormal(data, size, 0);
  }

  void overwriteNormal(const char *data, uint32_t size) {
    insertNormal(data, size, HAM_OVERWRITE);
  }

  void duplicateNormal(const char *data, uint32_t size) {
    insertNormal(data, size, HAM_DUPLICATE);
  }

  void resetPage() {
    PageManager *pm = m_dbp->get_local_env()->get_page_manager();
    pm->add_to_freelist(m_page);

    m_page = pm->alloc_page(m_dbp, Page::kTypeBindex,
                    PageManager::kClearWithZero);
    PBtreeNode *node = PBtreeNode::from_page(m_page);
    node->set_flags(PBtreeNode::kLeafNode);
  }

  void setRecordTest() {
    /* set empty record */
    prepareEmpty();

    /* set tiny record */
    prepareTiny("1234", 4);

    /* set small record */
    prepareSmall("12345678");

    /* set normal record */
    prepareNormal("1234567812345678", sizeof(uint64_t)*2);
  }

  void overwriteRecordTest() {
    /* overwrite empty record with a tiny key */
    prepareEmpty();
    overwriteTiny("1234", 4);

    /* overwrite empty record with an empty key */
    prepareEmpty();
    overwriteEmpty();

    /* overwrite empty record with a normal key */
    prepareEmpty();
    overwriteNormal("1234123456785678", 16);

    /* overwrite tiny record with an empty key */
    prepareTiny("1234", 4);
    overwriteEmpty();

    /* overwrite tiny record with a normal key */
    prepareTiny("1234", 4);
    overwriteNormal("1234123456785678", 16);

    /* overwrite small record with an empty key */
    prepareSmall("12341234");
    overwriteEmpty();

    /* overwrite small record with a normal key */
    prepareSmall("12341234");
    overwriteNormal("1234123456785678", 16);

    /* overwrite normal record with an empty key */
    prepareNormal("1234123456785678", 16);
    overwriteEmpty();

    /* overwrite normal record with a small key */
    prepareNormal("1234123456785678", 16);
    overwriteSmall("12341234");

    /* overwrite normal record with a tiny key */
    prepareNormal("1234123456785678", 16);
    overwriteTiny("1234", 4);

    /* overwrite normal record with a normal key */
    prepareNormal("1234123456785678", 16);
    overwriteNormal("1234123456785678", 16);
  }

  void checkDupe(int position, const char *data, uint32_t size) {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);
    int slot = 0;
    REQUIRE(node->get_record_count(slot) >= 1);

    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));

    ByteArray arena;
    node->get_record(slot, &arena, &rec, 0, position);
    REQUIRE(rec.size == size);
    if (size)
      REQUIRE(0 == memcmp(rec.data, data, rec.size));
    else
      REQUIRE((void *)0 == rec.data);
  }

  void duplicateRecordTest() {
    /* insert empty key, then another empty duplicate */
    prepareEmpty();
    duplicateEmpty();
    checkDupe(0, 0, 0);
    checkDupe(1, 0, 0);

    /* insert empty key, then another small duplicate */
    resetPage();
    prepareEmpty();
    duplicateSmall("12345678");
    checkDupe(0, 0, 0);
    checkDupe(1, "12345678", 8);

    /* insert empty key, then another tiny duplicate */
    resetPage();
    prepareEmpty();
    duplicateTiny("1234", 4);
    checkDupe(0, 0, 0);
    checkDupe(1, "1234", 4);

    /* insert empty key, then another normal duplicate */
    resetPage();
    prepareEmpty();
    duplicateNormal("1234567812345678", 16);
    checkDupe(0, 0, 0);
    checkDupe(1, "1234567812345678", 16);

    /* insert tiny key, then another empty duplicate */
    resetPage();
    prepareTiny("1234", 4);
    duplicateEmpty();
    checkDupe(0, "1234", 4);
    checkDupe(1, 0, 0);

    /* insert tiny key, then another small duplicate */
    resetPage();
    prepareTiny("1234", 4);
    duplicateSmall("12345678");
    checkDupe(0, "1234", 4);
    checkDupe(1, "12345678", 8);

    /* insert tiny key, then another tiny duplicate */
    resetPage();
    prepareTiny("1234", 4);
    duplicateTiny("23456", 5);
    checkDupe(0, "1234", 4);
    checkDupe(1, "23456", 5);

    /* insert tiny key, then another normal duplicate */
    resetPage();
    prepareTiny("1234", 4);
    duplicateNormal("1234567812345678", 16);
    checkDupe(0, "1234", 4);
    checkDupe(1, "1234567812345678", 16);

    /* insert small key, then another empty duplicate */
    resetPage();
    prepareSmall("12341234");
    duplicateEmpty();
    checkDupe(0, "12341234", 8);
    checkDupe(1, 0, 0);

    /* insert small key, then another small duplicate */
    resetPage();
    prepareSmall("xx341234");
    duplicateSmall("12345678");
    checkDupe(0, "xx341234", 8);
    checkDupe(1, "12345678", 8);

    /* insert small key, then another tiny duplicate */
    resetPage();
    prepareSmall("12341234");
    duplicateTiny("1234", 4);
    checkDupe(0, "12341234", 8);
    checkDupe(1, "1234", 4);

    /* insert small key, then another normal duplicate */
    resetPage();
    prepareSmall("12341234");
    duplicateNormal("1234567812345678", 16);
    checkDupe(0, "12341234", 8);
    checkDupe(1, "1234567812345678", 16);

    /* insert normal key, then another empty duplicate */
    resetPage();
    prepareNormal("1234123456785678", 16);
    duplicateEmpty();
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, 0, 0);

    /* insert normal key, then another small duplicate */
    resetPage();
    prepareNormal("1234123456785678", 16);
    duplicateSmall("12345678");
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "12345678", 8);

    /* insert normal key, then another tiny duplicate */
    resetPage();
    prepareNormal("1234123456785678", 16);
    duplicateTiny("1234", 4);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "1234", 4);

    /* insert normal key, then another normal duplicate */
    resetPage();
    prepareNormal("1234123456785678", 16);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "abc4567812345678", 16);
  }

  void eraseRecordTest() {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);

    /* insert empty key, then delete it */
    prepareEmpty();
    node->erase_record(0, 0, false, 0);
    REQUIRE((uint64_t)0 == node->get_record_id(0));

    /* insert tiny key, then delete it */
    prepareTiny("1234", 4);
    node->erase_record(0, 0, false, 0);
    REQUIRE((uint64_t)0 == node->get_record_id(0));

    /* insert small key, then delete it */
    prepareSmall("12345678");
    node->erase_record(0, 0, false, 0);
    REQUIRE((uint64_t)0 == node->get_record_id(0));

    /* insert normal key, then delete it */
    prepareNormal("1234123456785678", 16);
    node->erase_record(0, 0, false, 0);
    REQUIRE((uint64_t)0 == node->get_record_id(0));
  }

  void eraseDuplicateRecordTest1() {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);

    /* insert empty key, then a duplicate; delete both */
    prepareEmpty();
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, 0, 0);
    checkDupe(1, "abc4567812345678", 16);
    node->erase_record(0, 0, true, 0);
  }

  void eraseDuplicateRecordTest2() {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);

    /* insert tiny key, then a duplicate; delete both */
    prepareTiny("1234", 4);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234", 4);
    checkDupe(1, "abc4567812345678", 16);
    node->erase_record(0, 0, true, 0);
  }

  void eraseDuplicateRecordTest3() {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);

    /* insert small key, then a duplicate; delete both */
    prepareSmall("12345678");
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "12345678", 8);
    checkDupe(1, "abc4567812345678", 16);
    node->erase_record(0, 0, true, 0);
  }

  void eraseDuplicateRecordTest4() {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);

    /* insert normal key, then a duplicate; delete both */
    prepareNormal("1234123456785678", 16);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "abc4567812345678", 16);
    node->erase_record(0, 0, true, 0);
  }

  void eraseAllDuplicateRecordTest1() {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);

    /* insert empty key, then a duplicate; delete both at once */
    prepareEmpty();
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, 0, 0);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(node->get_record_count(0) == 2);
    node->erase_record(0, 0, false, 0);
    REQUIRE(node->get_record_count(0) == 1);
    checkDupe(0, "abc4567812345678", 16);
    node->erase_record(0, 0, false, 0);
  }

  void eraseAllDuplicateRecordTest2() {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);

    /* insert tiny key, then a duplicate; delete both */
    prepareTiny("1234", 4);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234", 4);
    checkDupe(1, "abc4567812345678", 16);
    node->erase_record(0, 1, false, 0);
    REQUIRE(node->get_record_count(0) == 1);
    checkDupe(0, "1234", 4);
    node->erase_record(0, 0, false, 0);
  }

  void eraseAllDuplicateRecordTest3() {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);

    /* insert small key, then a duplicate; delete both at once */
    prepareSmall("12345678");
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "12345678", 8);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(node->get_record_count(0) == 2);
    node->erase_record(0, 0, false, 0);
    REQUIRE(node->get_record_count(0) == 1);
    checkDupe(0, "abc4567812345678", 16);
    node->erase_record(0, 0, false, 0);
  }

  void eraseAllDuplicateRecordTest4() {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);

    /* insert normal key, then a duplicate; delete both at once */
    prepareNormal("1234123456785678", 16);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(node->get_record_count(0) == 2);
    node->erase_record(0, 1, false, 0);
    REQUIRE(node->get_record_count(0) == 1);
    checkDupe(0, "1234123456785678", 16);
    node->erase_record(0, 0, false, 0);
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

} // namespace hamsterdb
