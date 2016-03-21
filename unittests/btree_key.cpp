/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#include "3rdparty/catch/catch.hpp"

#include "utils.h"
#include "os.hpp"

#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3btree/btree_index.h"
#include "3btree/btree_flags.h"
#include "3btree/btree_node_proxy.h"
#include "3btree/btree_impl_default.h"
#include "3page_manager/page_manager.h"
#include "3btree/btree_node.h"
#include "4context/context.h"
#include "4db/db.h"
#include "4env/env_local.h"

namespace upscaledb {

struct BtreeKeyFixture {
  ups_db_t *m_db;
  LocalDb *m_dbp;
  ups_env_t *m_env;
  Page *m_page;
  ScopedPtr<Context> m_context;

  BtreeKeyFixture(bool duplicate = false) {
    os::unlink(Utils::opath(".test"));

    uint32_t flags = 0;
    if (duplicate)
      flags |= UPS_ENABLE_DUPLICATE_KEYS;
    REQUIRE(0 == ups_env_create(&m_env, Utils::opath(".test"), 0, 0644, 0));
    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 1, flags, 0));

    m_dbp = (LocalDb *)m_db;
    m_context.reset(new Context((LocalEnv *)m_env, 0, m_dbp));

    m_page = page_manager()->alloc(m_context.get(),
                    Page::kTypeBindex, PageManager::kClearWithZero);

    // this is a leaf page! internal pages cause different behavior... 
    PBtreeNode *node = PBtreeNode::from_page(m_page);
    node->set_flags(PBtreeNode::kLeafNode);
  }

  PageManager *page_manager() {
    LocalEnv *env = (LocalEnv *)m_dbp->env;
    return env->page_manager.get();
  }

  ~BtreeKeyFixture() {
    m_context->changeset.clear();

    if (m_env)
	  REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
  }

  void insertEmpty(uint32_t flags) {
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);
    ups_key_t key = {0};
    ups_record_t rec = {0};

    PBtreeNode::InsertResult result(0, 0);
    if (!flags)
      result = node->insert(m_context.get(), &key, 0);

    node->set_record(m_context.get(), result.slot, &rec, 0, flags, 0);
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
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);
    ByteArray arena;
    ups_record_t rec, rec2;
    ups_key_t key = {0};

    PBtreeNode::InsertResult result(0, 0);
    if (!flags)
      result = node->insert(m_context.get(), &key, 0);

    memset(&rec, 0, sizeof(rec));
    memset(&rec2, 0, sizeof(rec2));
    rec.data = (void *)data;
    rec.size = size;

    node->set_record(m_context.get(), result.slot, &rec, 0, flags, 0);

    if (!(flags & UPS_DUPLICATE)) {
      node->record(m_context.get(), result.slot, &arena, &rec2, 0);
      REQUIRE(rec.size == rec2.size);
      REQUIRE(0 == memcmp(rec.data, rec2.data, rec.size));
    }
    else
      REQUIRE(node->record_count(m_context.get(), result.slot) > 1);
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
    ByteArray arena;
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);
    ups_record_t rec, rec2;
    ups_key_t key = {0};

    PBtreeNode::InsertResult result(0, 0);
    if (!flags)
      result = node->insert(m_context.get(), &key, 0);

    memset(&rec, 0, sizeof(rec));
    memset(&rec2, 0, sizeof(rec2));
    rec.data = (void *)data;
    rec.size = sizeof(uint64_t);

    node->set_record(m_context.get(), result.slot, &rec, 0, flags, 0);
    if (flags & UPS_DUPLICATE) {
      REQUIRE(node->record_count(m_context.get(), result.slot) > 1);
    }

    if (!(flags & UPS_DUPLICATE)) {
      node->record(m_context.get(), result.slot, &arena, &rec2, 0);
      REQUIRE(rec.size == rec2.size);
      REQUIRE(0 == memcmp(rec.data, rec2.data, rec.size));
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
    ByteArray arena;
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);
    ups_record_t rec, rec2;
    ups_key_t key = {0};

    PBtreeNode::InsertResult result(0, 0);
    if (!flags)
      result = node->insert(m_context.get(), &key, 0);

    memset(&rec, 0, sizeof(rec));
    memset(&rec2, 0, sizeof(rec2));
    rec.data = (void *)data;
    rec.size = size;

    node->set_record(m_context.get(), result.slot, &rec, 0, flags, 0);
    if (flags & UPS_DUPLICATE)
      REQUIRE(node->record_count(m_context.get(), result.slot) > 1);

    if (!(flags & UPS_DUPLICATE)) {
      node->record(m_context.get(), result.slot, &arena, &rec2, 0);
      REQUIRE(rec.size == rec2.size);
      REQUIRE(0 == memcmp(rec.data, rec2.data, rec.size));
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
    page_manager()->del(m_context.get(), m_page);

    m_page = page_manager()->alloc(m_context.get(), Page::kTypeBindex,
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
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);
    int slot = 0;
    REQUIRE(node->record_count(m_context.get(), slot) >= 1);

    ups_record_t rec;
    memset(&rec, 0, sizeof(rec));

    ByteArray arena;
    node->record(m_context.get(), slot, &arena, &rec, 0, position);
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
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);

    /* insert empty key, then delete it */
    prepareEmpty();
    node->erase_record(m_context.get(), 0, 0, false, 0);
    REQUIRE((uint64_t)0 == node->record_id(m_context.get(), 0));

    /* insert tiny key, then delete it */
    prepareTiny("1234", 4);
    node->erase_record(m_context.get(), 0, 0, false, 0);
    REQUIRE((uint64_t)0 == node->record_id(m_context.get(), 0));

    /* insert small key, then delete it */
    prepareSmall("12345678");
    node->erase_record(m_context.get(), 0, 0, false, 0);
    REQUIRE((uint64_t)0 == node->record_id(m_context.get(), 0));

    /* insert normal key, then delete it */
    prepareNormal("1234123456785678", 16);
    node->erase_record(m_context.get(), 0, 0, false, 0);
    REQUIRE((uint64_t)0 == node->record_id(m_context.get(), 0));
  }

  void eraseDuplicateRecordTest1() {
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);

    /* insert empty key, then a duplicate; delete both */
    prepareEmpty();
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, 0, 0);
    checkDupe(1, "abc4567812345678", 16);
    node->erase_record(m_context.get(), 0, 0, true, 0);
  }

  void eraseDuplicateRecordTest2() {
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);

    /* insert tiny key, then a duplicate; delete both */
    prepareTiny("1234", 4);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234", 4);
    checkDupe(1, "abc4567812345678", 16);
    node->erase_record(m_context.get(), 0, 0, true, 0);
  }

  void eraseDuplicateRecordTest3() {
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);

    /* insert small key, then a duplicate; delete both */
    prepareSmall("12345678");
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "12345678", 8);
    checkDupe(1, "abc4567812345678", 16);
    node->erase_record(m_context.get(), 0, 0, true, 0);
  }

  void eraseDuplicateRecordTest4() {
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);

    /* insert normal key, then a duplicate; delete both */
    prepareNormal("1234123456785678", 16);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "abc4567812345678", 16);
    node->erase_record(m_context.get(), 0, 0, true, 0);
  }

  void eraseAllDuplicateRecordTest1() {
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);

    /* insert empty key, then a duplicate; delete both at once */
    prepareEmpty();
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, 0, 0);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(node->record_count(m_context.get(), 0) == 2);
    node->erase_record(m_context.get(), 0, 0, false, 0);
    REQUIRE(node->record_count(m_context.get(), 0) == 1);
    checkDupe(0, "abc4567812345678", 16);
    node->erase_record(m_context.get(), 0, 0, false, 0);
  }

  void eraseAllDuplicateRecordTest2() {
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);

    /* insert tiny key, then a duplicate; delete both */
    prepareTiny("1234", 4);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234", 4);
    checkDupe(1, "abc4567812345678", 16);
    node->erase_record(m_context.get(), 0, 1, false, 0);
    REQUIRE(node->record_count(m_context.get(), 0) == 1);
    checkDupe(0, "1234", 4);
    node->erase_record(m_context.get(), 0, 0, false, 0);
  }

  void eraseAllDuplicateRecordTest3() {
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);

    /* insert small key, then a duplicate; delete both at once */
    prepareSmall("12345678");
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "12345678", 8);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(node->record_count(m_context.get(), 0) == 2);
    node->erase_record(m_context.get(), 0, 0, false, 0);
    REQUIRE(node->record_count(m_context.get(), 0) == 1);
    checkDupe(0, "abc4567812345678", 16);
    node->erase_record(m_context.get(), 0, 0, false, 0);
  }

  void eraseAllDuplicateRecordTest4() {
    BtreeNodeProxy *node = m_dbp->btree_index->get_node_from_page(m_page);

    /* insert normal key, then a duplicate; delete both at once */
    prepareNormal("1234123456785678", 16);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(node->record_count(m_context.get(), 0) == 2);
    node->erase_record(m_context.get(), 0, 1, false, 0);
    REQUIRE(node->record_count(m_context.get(), 0) == 1);
    checkDupe(0, "1234123456785678", 16);
    node->erase_record(m_context.get(), 0, 0, false, 0);
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
