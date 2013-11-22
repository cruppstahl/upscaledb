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

#include "3rdparty/catch/catch.hpp"

#include "globals.h"
#include "os.hpp"

#include "../src/db.h"
#include "../src/btree_index.h"
#include "../src/btree_key.h"
#include "../src/btree_node_proxy.h"
#include "../src/btree_node_default.h"
#include "../src/util.h"
#include "../src/page.h"
#include "../src/env_local.h"
#include "../src/btree_node.h"

namespace hamsterdb {

struct BtreeKeyFixture {
  ham_db_t *m_db;
  LocalDatabase *m_dbp;
  ham_env_t *m_env;
  Page *m_page;

  BtreeKeyFixture() {
    os::unlink(Globals::opath(".test"));

    REQUIRE(0 == ham_env_create(&m_env, Globals::opath(".test"), 0, 0644, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, 0));

    m_dbp = (LocalDatabase *)m_db;

    m_page = new Page((LocalEnvironment *)m_env);
    m_page->set_db(m_dbp);
    REQUIRE(0 == m_page->allocate());

    // make sure that the node is properly initialized
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);
    node->test_clear_page();
  }

  ~BtreeKeyFixture() {
    if (m_page)
      delete m_page;
    if (m_env)
	  REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void insertEmpty(ham_u32_t flags) {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);
    int slot = 0;
    ham_record_t rec;

    if (!flags)
      node->test_clear_page();
    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == node->set_record(slot, 0, &rec, 0, flags, 0));
    if (!(flags & HAM_DUPLICATE)) {
      REQUIRE((ham_u8_t)BtreeKey::kBlobSizeEmpty == node->test_get_flags(slot));
    }
    else {
      REQUIRE((ham_u8_t)BtreeKey::kDuplicates == node->test_get_flags(slot));
    }
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

  void insertTiny(const char *data, ham_u32_t size, ham_u32_t flags) {
    ByteArray arena;
    ham_record_t rec, rec2;
    int slot = 0;

    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);
    if (!flags)
      node->test_clear_page();

    memset(&rec, 0, sizeof(rec));
    memset(&rec2, 0, sizeof(rec2));
    rec.data = (void *)data;
    rec.size = size;

    REQUIRE(0 == node->set_record(slot, 0, &rec, 0, flags, 0));
    if (!(flags & HAM_DUPLICATE))
      REQUIRE((ham_u8_t)BtreeKey::kBlobSizeTiny ==
                      node->test_get_flags(slot));
    else
      REQUIRE((ham_u8_t)BtreeKey::kDuplicates ==
                      node->test_get_flags(slot));

    if (!(flags & HAM_DUPLICATE)) {
      REQUIRE(0 == node->get_record(slot, &arena, &rec2, 0));
      REQUIRE(rec.size == rec2.size);
      REQUIRE(0 == memcmp(rec.data, rec2.data, rec.size));
    }
  }

  void prepareTiny(const char *data, ham_u32_t size) {
    ByteArray arena;
    insertTiny(data, size, 0);
  }

  void overwriteTiny(const char *data, ham_u32_t size) {
    insertTiny(data, size, HAM_OVERWRITE);
  }

  void duplicateTiny(const char *data, ham_u32_t size) {
    insertTiny(data, size, HAM_DUPLICATE);
  }

  void insertSmall(const char *data, ham_u32_t flags) {
    ByteArray arena;
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);
    int slot = 0;
    ham_record_t rec, rec2;

    if (!flags)
      node->test_clear_page();
    memset(&rec, 0, sizeof(rec));
    memset(&rec2, 0, sizeof(rec2));
    rec.data = (void *)data;
    rec.size = sizeof(ham_u64_t);

    REQUIRE(0 == node->set_record(slot, 0, &rec, 0, flags, 0));
    if (!(flags & HAM_DUPLICATE)) {
      REQUIRE((ham_u8_t)BtreeKey::kBlobSizeSmall ==
                      node->test_get_flags(slot));
    }
    else {
      REQUIRE((ham_u8_t)BtreeKey::kDuplicates ==
                      node->test_get_flags(slot));
    }

    if (!(flags & HAM_DUPLICATE)) {
      REQUIRE(0 == node->get_record(slot, &arena, &rec2, 0));
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

  void insertNormal(const char *data, ham_u32_t size, ham_u32_t flags) {
    ByteArray arena;
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);
    int slot = 0;
    ham_record_t rec, rec2;

    if (!flags)
      node->test_clear_page();
    memset(&rec, 0, sizeof(rec));
    memset(&rec2, 0, sizeof(rec2));
    rec.data = (void *)data;
    rec.size = size;

    REQUIRE(0 == node->set_record(slot, 0, &rec, 0, flags, 0));
    if (flags & HAM_DUPLICATE)
      REQUIRE((ham_u8_t)BtreeKey::kDuplicates ==
                      node->test_get_flags(slot));

    if (!(flags & HAM_DUPLICATE)) {
      REQUIRE(0 == node->get_record(slot, &arena, &rec2, 0));
      REQUIRE(rec.size == rec2.size);
      REQUIRE(0 == memcmp(rec.data, rec2.data, rec.size));
    }
  }

  void prepareNormal(const char *data, ham_u32_t size) {
    insertNormal(data, size, 0);
  }

  void overwriteNormal(const char *data, ham_u32_t size) {
    insertNormal(data, size, HAM_OVERWRITE);
  }

  void duplicateNormal(const char *data, ham_u32_t size) {
    insertNormal(data, size, HAM_DUPLICATE);
  }

  void setRecordTest() {
    /* set empty record */
    prepareEmpty();

    /* set tiny record */
    prepareTiny("1234", 4);

    /* set small record */
    prepareSmall("12345678");

    /* set normal record */
    prepareNormal("1234567812345678", sizeof(ham_u64_t)*2);
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

  void checkDupe(int position, const char *data, ham_u32_t size) {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);
    int slot = 0;
    REQUIRE((ham_u8_t)BtreeKey::kDuplicates == node->test_get_flags(slot));

    PDupeEntry entry;
    DuplicateManager *dm = ((LocalEnvironment *)m_env)->get_duplicate_manager();
    REQUIRE(0 == dm->get(node->get_record_id(slot),
                            (ham_u32_t)position, &entry));

    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));

    ByteArray arena;
    REQUIRE(0 == node->get_record(slot, &arena, &rec, 0, position));
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
    prepareEmpty();
    duplicateSmall("12345678");
    checkDupe(0, 0, 0);
    checkDupe(1, "12345678", 8);

    /* insert empty key, then another tiny duplicate */
    prepareEmpty();
    duplicateTiny("1234", 4);
    checkDupe(0, 0, 0);
    checkDupe(1, "1234", 4);

    /* insert empty key, then another normal duplicate */
    prepareEmpty();
    duplicateNormal("1234567812345678", 16);
    checkDupe(0, 0, 0);
    checkDupe(1, "1234567812345678", 16);

    /* insert tiny key, then another empty duplicate */
    prepareTiny("1234", 4);
    duplicateEmpty();
    checkDupe(0, "1234", 4);
    checkDupe(1, 0, 0);

    /* insert tiny key, then another small duplicate */
    prepareTiny("1234", 4);
    duplicateSmall("12345678");
    checkDupe(0, "1234", 4);
    checkDupe(1, "12345678", 8);

    /* insert tiny key, then another tiny duplicate */
    prepareTiny("1234", 4);
    duplicateTiny("23456", 5);
    checkDupe(0, "1234", 4);
    checkDupe(1, "23456", 5);

    /* insert tiny key, then another normal duplicate */
    prepareTiny("1234", 4);
    duplicateNormal("1234567812345678", 16);
    checkDupe(0, "1234", 4);
    checkDupe(1, "1234567812345678", 16);

    /* insert small key, then another empty duplicate */
    prepareSmall("12341234");
    duplicateEmpty();
    checkDupe(0, "12341234", 8);
    checkDupe(1, 0, 0);

    /* insert small key, then another small duplicate */
    prepareSmall("xx341234");
    duplicateSmall("12345678");
    checkDupe(0, "xx341234", 8);
    checkDupe(1, "12345678", 8);

    /* insert small key, then another tiny duplicate */
    prepareSmall("12341234");
    duplicateTiny("1234", 4);
    checkDupe(0, "12341234", 8);
    checkDupe(1, "1234", 4);

    /* insert small key, then another normal duplicate */
    prepareSmall("12341234");
    duplicateNormal("1234567812345678", 16);
    checkDupe(0, "12341234", 8);
    checkDupe(1, "1234567812345678", 16);

    /* insert normal key, then another empty duplicate */
    prepareNormal("1234123456785678", 16);
    duplicateEmpty();
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, 0, 0);

    /* insert normal key, then another small duplicate */
    prepareNormal("1234123456785678", 16);
    duplicateSmall("12345678");
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "12345678", 8);

    /* insert normal key, then another tiny duplicate */
    prepareNormal("1234123456785678", 16);
    duplicateTiny("1234", 4);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "1234", 4);

    /* insert normal key, then another normal duplicate */
    prepareNormal("1234123456785678", 16);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "abc4567812345678", 16);
  }

  void eraseRecordTest() {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);

    /* insert empty key, then delete it */
    prepareEmpty();
    REQUIRE(0 == node->erase_record(0, 0, false, 0));
    REQUIRE((ham_u8_t)0 == node->test_get_flags(0));
    REQUIRE((ham_u64_t)0 == node->get_record_id(0));

    /* insert tiny key, then delete it */
    prepareTiny("1234", 4);
    REQUIRE(0 == node->erase_record(0, 0, false, 0));
    REQUIRE((ham_u8_t)0 == node->test_get_flags(0));
    REQUIRE((ham_u64_t)0 == node->get_record_id(0));

    /* insert small key, then delete it */
    prepareSmall("12345678");
    REQUIRE(0 == node->erase_record(0, 0, false, 0));
    REQUIRE((ham_u8_t)0 == node->test_get_flags(0));
    REQUIRE((ham_u64_t)0 == node->get_record_id(0));

    /* insert normal key, then delete it */
    prepareNormal("1234123456785678", 16);
    REQUIRE(0 == node->erase_record(0, 0, false, 0));
    REQUIRE((ham_u8_t)0 == node->test_get_flags(0));
    REQUIRE((ham_u64_t)0 == node->get_record_id(0));
  }

  void eraseDuplicateRecordTest() {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);

    /* insert empty key, then a duplicate; delete both */
    prepareEmpty();
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, 0, 0);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(0 == node->erase_record(0, 0, true, 0));
    REQUIRE((ham_u8_t)0 == node->test_get_flags(0));
    REQUIRE((ham_u64_t)0 == node->get_record_id(0));

    /* insert tiny key, then a duplicate; delete both */
    prepareTiny("1234", 4);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234", 4);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(0 == node->erase_record(0, 0, true, 0));
    REQUIRE((ham_u8_t)0 == node->test_get_flags(0));
    REQUIRE((ham_u64_t)0 == node->get_record_id(0));

    /* insert small key, then a duplicate; delete both */
    prepareSmall("12345678");
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "12345678", 8);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(0 == node->erase_record(0, 0, true, 0));
    REQUIRE((ham_u8_t)0 == node->test_get_flags(0));
    REQUIRE((ham_u64_t)0 == node->get_record_id(0));

    /* insert normal key, then a duplicate; delete both */
    prepareNormal("1234123456785678", 16);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(0 == node->erase_record(0, 0, true, 0));
    REQUIRE((ham_u8_t)0 == node->test_get_flags(0));
    REQUIRE((ham_u64_t)0 == node->get_record_id(0));
  }

  void eraseAllDuplicateRecordTest() {
    BtreeNodeProxy *node = m_dbp->get_btree_index()->get_node_from_page(m_page);

    /* insert empty key, then a duplicate; delete both at once */
    prepareEmpty();
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, 0, 0);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE((ham_u8_t)BtreeKey::kDuplicates == node->test_get_flags(0));
    REQUIRE(0 == node->erase_record(0, 0, false, 0));
    REQUIRE((ham_u8_t)BtreeKey::kDuplicates == node->test_get_flags(0));
    checkDupe(0, "abc4567812345678", 16);
    REQUIRE(0 == node->erase_record(0, 0, false, 0));
    REQUIRE((ham_u8_t)0 == node->test_get_flags(0));
    REQUIRE((ham_u64_t)0 == node->get_record_id(0));

    /* insert tiny key, then a duplicate; delete both */
    prepareTiny("1234", 4);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234", 4);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(0 == node->erase_record(0, 1, false, 0));
    REQUIRE((ham_u8_t)BtreeKey::kDuplicates == node->test_get_flags(0));
    checkDupe(0, "1234", 4);
    REQUIRE(0 == node->erase_record(0, 0, false, 0));
    REQUIRE((ham_u8_t)0 == node->test_get_flags(0));
    REQUIRE((ham_u64_t)0 == node->get_record_id(0));

    /* insert small key, then a duplicate; delete both at once */
    prepareSmall("12345678");
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "12345678", 8);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(0 == node->erase_record(0, 0, false, 0));
    REQUIRE((ham_u8_t)BtreeKey::kDuplicates == node->test_get_flags(0));
    checkDupe(0, "abc4567812345678", 16);
    REQUIRE(0 == node->erase_record(0, 0, false, 0));
    REQUIRE((ham_u8_t)0 == node->test_get_flags(0));
    REQUIRE((ham_u64_t)0 == node->get_record_id(0));

    /* insert normal key, then a duplicate; delete both at once */
    prepareNormal("1234123456785678", 16);
    duplicateNormal("abc4567812345678", 16);
    checkDupe(0, "1234123456785678", 16);
    checkDupe(1, "abc4567812345678", 16);
    REQUIRE(0 == node->erase_record(0, 1, false, 0));
    REQUIRE((ham_u8_t)BtreeKey::kDuplicates == node->test_get_flags(0));
    checkDupe(0, "1234123456785678", 16);
    REQUIRE(0 == node->erase_record(0, 0, false, 0));
    REQUIRE((ham_u8_t)0 == node->test_get_flags(0));
    REQUIRE((ham_u64_t)0 == node->get_record_id(0));
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
  BtreeKeyFixture f;
  f.duplicateRecordTest();
}

TEST_CASE("BtreeKey/eraseRecord", "")
{
  BtreeKeyFixture f;
  f.eraseRecordTest();
}

TEST_CASE("BtreeKey/eraseDuplicateRecord", "")
{
  BtreeKeyFixture f;
  f.eraseDuplicateRecordTest();
}

TEST_CASE("BtreeKey/eraseAllDuplicateRecord", "")
{
  BtreeKeyFixture f;
  f.eraseAllDuplicateRecordTest();
}

} // namespace hamsterdb
