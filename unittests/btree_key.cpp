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
#include "../src/btree.h"
#include "../src/btree_key.h"
#include "../src/util.h"
#include "../src/page.h"
#include "../src/env.h"
#include "../src/btree_node.h"

namespace hamsterdb {

struct BtreeKeyFixture {
  ham_db_t *m_db;
  Database *m_dbp;
  ham_env_t *m_env;

  BtreeKeyFixture() {
    os::unlink(Globals::opath(".test"));

    REQUIRE(0 == ham_env_create(&m_env, Globals::opath(".test"), 0, 0644, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, 0));

    m_dbp = (Database *)m_db;
  }

  ~BtreeKeyFixture() {
    if (m_env)
	  REQUIRE(0 == ham_env_close(m_env, 0));
  }

  void structureTest() {
    Page *page = new Page((Environment *)m_env);
    REQUIRE(page != 0);
    REQUIRE(0 == page->allocate());
    PBtreeNode *node = PBtreeNode::from_page(page);
    ::memset(node, 0, ((Environment *)m_env)->get_usable_pagesize());

    PBtreeKey *key = node->get_key(m_dbp, 0);
    REQUIRE((ham_u64_t)0 == key->get_ptr());
    REQUIRE((ham_u8_t)0 == key->get_flags());
    REQUIRE((ham_u8_t)'\0' == *key->get_key());

    key->set_ptr((ham_u64_t)0x12345);
    REQUIRE((ham_u64_t)0x12345 == key->get_ptr());

    key->set_flags((ham_u8_t)0x13);
    REQUIRE((ham_u8_t)0x13 == key->get_flags());

    page->free();
    delete page;
  }

  void extendedRidTest() {
    Page *page = new Page((Environment *)m_env);
    REQUIRE(page != 0);
    REQUIRE(0 == page->allocate());
    PBtreeNode *node = PBtreeNode::from_page(page);
    ::memset(node, 0, ((Environment *)m_env)->get_usable_pagesize());

    PBtreeKey *key = node->get_key(m_dbp, 0);
    ham_u64_t blobid = key->get_extended_rid(m_dbp);
    REQUIRE((ham_u64_t)0 == blobid);

    key->set_extended_rid(m_dbp, (ham_u64_t)0xbaadbeef);
    blobid = key->get_extended_rid(m_dbp);
    REQUIRE((ham_u64_t)0xbaadbeef == blobid);

    page->free();
    delete page;
  }

  void endianTest() {
    ham_u8_t buffer[64] = {
        0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
        0x00, 0x00, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };

    PBtreeKey *key = (PBtreeKey *)&buffer[0];

    REQUIRE((ham_u64_t)0x0123456789abcdefull ==
        key->get_ptr());
    REQUIRE((ham_u8_t)0xf0 == key->get_flags());
    REQUIRE((ham_u64_t)0xfedcba9876543210ull ==
        key->get_extended_rid(m_dbp));
  }

  void getSetExtendedKeyTest() {
    char buffer[32];
    PBtreeKey *key = (PBtreeKey *)buffer;
    memset(buffer, 0, sizeof(buffer));

    key->set_extended_rid(m_dbp, 0x12345);
    REQUIRE((ham_u64_t)0x12345 == key->get_extended_rid(m_dbp));
  }

  void insertEmpty(PBtreeKey *key, ham_u32_t flags) {
    ham_record_t rec;

    if (!flags)
      memset(key, 0, sizeof(*key));
    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == key->set_record(m_dbp, 0, &rec, 0, flags, 0));
    if (!(flags & HAM_DUPLICATE))
      REQUIRE((ham_u64_t)0 == key->get_ptr());

    if (!(flags & HAM_DUPLICATE)) {
      REQUIRE((ham_u8_t)PBtreeKey::KEY_BLOB_SIZE_EMPTY ==
          key->get_flags());
    }
    else {
      REQUIRE((ham_u8_t)PBtreeKey::KEY_HAS_DUPLICATES ==
          key->get_flags());
    }
  }

  void prepareEmpty(PBtreeKey *key) {
    insertEmpty(key, 0);
  }

  void overwriteEmpty(PBtreeKey *key) {
    insertEmpty(key, HAM_OVERWRITE);
  }

  void duplicateEmpty(PBtreeKey *key) {
    insertEmpty(key, HAM_DUPLICATE);
  }

  void insertTiny(PBtreeKey *key, const char *data, ham_size_t size,
      ham_u32_t flags) {
    ham_record_t rec, rec2;

    if (!flags)
      memset(key, 0, sizeof(*key));
    memset(&rec, 0, sizeof(rec));
    memset(&rec2, 0, sizeof(rec2));
    rec.data = (void *)data;
    rec.size = size;

    REQUIRE(0 == key->set_record(m_dbp, 0, &rec, 0, flags, 0));
    if (!(flags & HAM_DUPLICATE))
      REQUIRE((ham_u8_t)PBtreeKey::KEY_BLOB_SIZE_TINY ==
        key->get_flags());
    else
      REQUIRE((ham_u8_t)PBtreeKey::KEY_HAS_DUPLICATES ==
          key->get_flags());

    if (!(flags & HAM_DUPLICATE)) {
      rec2._intflags = key->get_flags();
      rec2._rid = key->get_ptr();
      REQUIRE(0 == m_dbp->get_btree()->read_record(0,
            &rec2, &rec2._rid, 0));
      REQUIRE(rec.size == rec2.size);
      REQUIRE(0 == memcmp(rec.data, rec2.data, rec.size));
    }
  }

  void prepareTiny(PBtreeKey *key, const char *data, ham_size_t size) {
    insertTiny(key, data, size, 0);
  }

  void overwriteTiny(PBtreeKey *key, const char *data, ham_size_t size) {
    insertTiny(key, data, size, HAM_OVERWRITE);
  }

  void duplicateTiny(PBtreeKey *key, const char *data, ham_size_t size) {
    insertTiny(key, data, size, HAM_DUPLICATE);
  }

  void insertSmall(PBtreeKey *key, const char *data, ham_u32_t flags) {
    ham_record_t rec, rec2;

    if (!flags)
      memset(key, 0, sizeof(*key));
    memset(&rec, 0, sizeof(rec));
    memset(&rec2, 0, sizeof(rec2));
    rec.data = (void *)data;
    rec.size = sizeof(ham_u64_t);

    REQUIRE(0 == key->set_record(m_dbp, 0, &rec, 0, flags, 0));
    if (!(flags & HAM_DUPLICATE)) {
      REQUIRE((ham_u8_t)PBtreeKey::KEY_BLOB_SIZE_SMALL ==
        key->get_flags());
    }
    else {
      REQUIRE((ham_u8_t)PBtreeKey::KEY_HAS_DUPLICATES ==
          key->get_flags());
    }

    if (!(flags & HAM_DUPLICATE)) {
      rec2._intflags = key->get_flags();
      rec2._rid = key->get_ptr();
      REQUIRE(0 == m_dbp->get_btree()->read_record(0,
            &rec2, &rec2._rid, 0));
      REQUIRE(rec.size == rec2.size);
      REQUIRE(0 == memcmp(rec.data, rec2.data, rec.size));
    }
  }

  void prepareSmall(PBtreeKey *key, const char *data) {
    insertSmall(key, data, 0);
  }

  void overwriteSmall(PBtreeKey *key, const char *data) {
    insertSmall(key, data, HAM_OVERWRITE);
  }

  void duplicateSmall(PBtreeKey *key, const char *data) {
    insertSmall(key, data, HAM_DUPLICATE);
  }

  void insertNormal(PBtreeKey *key, const char *data, ham_size_t size,
      ham_u32_t flags) {
    ham_record_t rec, rec2;

    if (!flags)
      memset(key, 0, sizeof(*key));
    memset(&rec, 0, sizeof(rec));
    memset(&rec2, 0, sizeof(rec2));
    rec.data = (void *)data;
    rec.size = size;

    REQUIRE(0 == key->set_record(m_dbp, 0, &rec, 0, flags, 0));
    if (flags & HAM_DUPLICATE)
      REQUIRE((ham_u8_t)PBtreeKey::KEY_HAS_DUPLICATES ==
          key->get_flags());

    if (!(flags & HAM_DUPLICATE)) {
      rec2._intflags = key->get_flags();
      rec2._rid = key->get_ptr();
      REQUIRE(0 == m_dbp->get_btree()->read_record(0,
          &rec2, &rec2._rid, 0));
      REQUIRE(rec.size == rec2.size);
      REQUIRE(0 == memcmp(rec.data, rec2.data, rec.size));
    }
  }

  void prepareNormal(PBtreeKey *key, const char *data, ham_size_t size) {
    insertNormal(key, data, size, 0);
  }

  void overwriteNormal(PBtreeKey *key, const char *data, ham_size_t size) {
    insertNormal(key, data, size, HAM_OVERWRITE);
  }

  void duplicateNormal(PBtreeKey *key, const char *data, ham_size_t size) {
    insertNormal(key, data, size, HAM_DUPLICATE);
  }

  void setRecordTest(void) {
    PBtreeKey key;

    /* set empty record */
    prepareEmpty(&key);

    /* set tiny record */
    prepareTiny(&key, "1234", 4);

    /* set small record */
    prepareSmall(&key, "12345678");

    /* set normal record */
    prepareNormal(&key, "1234567812345678", sizeof(ham_u64_t)*2);
  }

  void overwriteRecordTest() {
    PBtreeKey key;

    /* overwrite empty record with a tiny key */
    prepareEmpty(&key);
    overwriteTiny(&key, "1234", 4);

    /* overwrite empty record with an empty key */
    prepareEmpty(&key);
    overwriteEmpty(&key);

    /* overwrite empty record with a normal key */
    prepareEmpty(&key);
    overwriteNormal(&key, "1234123456785678", 16);

    /* overwrite tiny record with an empty key */
    prepareTiny(&key, "1234", 4);
    overwriteEmpty(&key);

    /* overwrite tiny record with a normal key */
    prepareTiny(&key, "1234", 4);
    overwriteNormal(&key, "1234123456785678", 16);

    /* overwrite small record with an empty key */
    prepareSmall(&key, "12341234");
    overwriteEmpty(&key);

    /* overwrite small record with a normal key */
    prepareSmall(&key, "12341234");
    overwriteNormal(&key, "1234123456785678", 16);

    /* overwrite normal record with an empty key */
    prepareNormal(&key, "1234123456785678", 16);
    overwriteEmpty(&key);

    /* overwrite normal record with a small key */
    prepareNormal(&key, "1234123456785678", 16);
    overwriteSmall(&key, "12341234");

    /* overwrite normal record with a tiny key */
    prepareNormal(&key, "1234123456785678", 16);
    overwriteTiny(&key, "1234", 4);

    /* overwrite normal record with a normal key */
    prepareNormal(&key, "1234123456785678", 16);
    overwriteNormal(&key, "1234123456785678", 16);
  }

  void checkDupe(PBtreeKey *key, int position,
      const char *data, ham_size_t size) {
    REQUIRE((ham_u8_t)PBtreeKey::KEY_HAS_DUPLICATES == key->get_flags());

    PDupeEntry entry;
    DuplicateManager *dm = ((Environment *)m_env)->get_duplicate_manager();
    REQUIRE(0 == dm->get(key->get_ptr(), (ham_size_t)position, &entry));

    ham_record_t rec;
    memset(&rec, 0, sizeof(rec));

    rec._intflags = dupe_entry_get_flags(&entry);
    rec._rid = dupe_entry_get_rid(&entry);
    REQUIRE(0 == m_dbp->get_btree()->read_record(0, &rec, &rec._rid, 0));
    REQUIRE(rec.size == size);
    if (size)
      REQUIRE(0 == memcmp(rec.data, data, rec.size));
    else
      REQUIRE((void *)0 == rec.data);
  }

  void duplicateRecordTest() {
    PBtreeKey key;

    /* insert empty key, then another empty duplicate */
    prepareEmpty(&key);
    duplicateEmpty(&key);
    checkDupe(&key, 0, 0, 0);
    checkDupe(&key, 1, 0, 0);

    /* insert empty key, then another small duplicate */
    prepareEmpty(&key);
    duplicateSmall(&key, "12345678");
    checkDupe(&key, 0, 0, 0);
    checkDupe(&key, 1, "12345678", 8);

    /* insert empty key, then another tiny duplicate */
    prepareEmpty(&key);
    duplicateTiny(&key, "1234", 4);
    checkDupe(&key, 0, 0, 0);
    checkDupe(&key, 1, "1234", 4);

    /* insert empty key, then another normal duplicate */
    prepareEmpty(&key);
    duplicateNormal(&key, "1234567812345678", 16);
    checkDupe(&key, 0, 0, 0);
    checkDupe(&key, 1, "1234567812345678", 16);

    /* insert tiny key, then another empty duplicate */
    prepareTiny(&key, "1234", 4);
    duplicateEmpty(&key);
    checkDupe(&key, 0, "1234", 4);
    checkDupe(&key, 1, 0, 0);

    /* insert tiny key, then another small duplicate */
    prepareTiny(&key, "1234", 4);
    duplicateSmall(&key, "12345678");
    checkDupe(&key, 0, "1234", 4);
    checkDupe(&key, 1, "12345678", 8);

    /* insert tiny key, then another tiny duplicate */
    prepareTiny(&key, "1234", 4);
    duplicateTiny(&key, "23456", 5);
    checkDupe(&key, 0, "1234", 4);
    checkDupe(&key, 1, "23456", 5);

    /* insert tiny key, then another normal duplicate */
    prepareTiny(&key, "1234", 4);
    duplicateNormal(&key, "1234567812345678", 16);
    checkDupe(&key, 0, "1234", 4);
    checkDupe(&key, 1, "1234567812345678", 16);

    /* insert small key, then another empty duplicate */
    prepareSmall(&key, "12341234");
    duplicateEmpty(&key);
    checkDupe(&key, 0, "12341234", 8);
    checkDupe(&key, 1, 0, 0);

    /* insert small key, then another small duplicate */
    prepareSmall(&key, "xx341234");
    duplicateSmall(&key, "12345678");
    checkDupe(&key, 0, "xx341234", 8);
    checkDupe(&key, 1, "12345678", 8);

    /* insert small key, then another tiny duplicate */
    prepareSmall(&key, "12341234");
    duplicateTiny(&key, "1234", 4);
    checkDupe(&key, 0, "12341234", 8);
    checkDupe(&key, 1, "1234", 4);

    /* insert small key, then another normal duplicate */
    prepareSmall(&key, "12341234");
    duplicateNormal(&key, "1234567812345678", 16);
    checkDupe(&key, 0, "12341234", 8);
    checkDupe(&key, 1, "1234567812345678", 16);

    /* insert normal key, then another empty duplicate */
    prepareNormal(&key, "1234123456785678", 16);
    duplicateEmpty(&key);
    checkDupe(&key, 0, "1234123456785678", 16);
    checkDupe(&key, 1, 0, 0);

    /* insert normal key, then another small duplicate */
    prepareNormal(&key, "1234123456785678", 16);
    duplicateSmall(&key, "12345678");
    checkDupe(&key, 0, "1234123456785678", 16);
    checkDupe(&key, 1, "12345678", 8);

    /* insert normal key, then another tiny duplicate */
    prepareNormal(&key, "1234123456785678", 16);
    duplicateTiny(&key, "1234", 4);
    checkDupe(&key, 0, "1234123456785678", 16);
    checkDupe(&key, 1, "1234", 4);

    /* insert normal key, then another normal duplicate */
    prepareNormal(&key, "1234123456785678", 16);
    duplicateNormal(&key, "abc4567812345678", 16);
    checkDupe(&key, 0, "1234123456785678", 16);
    checkDupe(&key, 1, "abc4567812345678", 16);
  }

  void eraseRecordTest() {
    PBtreeKey key;

    /* insert empty key, then delete it */
    prepareEmpty(&key);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, false));
    REQUIRE((ham_u8_t)0 == key.get_flags());
    REQUIRE((ham_u64_t)0 == key.get_ptr());

    /* insert tiny key, then delete it */
    prepareTiny(&key, "1234", 4);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, false));
    REQUIRE((ham_u8_t)0 == key.get_flags());
    REQUIRE((ham_u64_t)0 == key.get_ptr());

    /* insert small key, then delete it */
    prepareSmall(&key, "12345678");
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, false));
    REQUIRE((ham_u8_t)0 == key.get_flags());
    REQUIRE((ham_u64_t)0 == key.get_ptr());

    /* insert normal key, then delete it */
    prepareNormal(&key, "1234123456785678", 16);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, false));
    REQUIRE((ham_u8_t)0 == key.get_flags());
    REQUIRE((ham_u64_t)0 == key.get_ptr());
  }

  void eraseDuplicateRecordTest() {
    PBtreeKey key;

    /* insert empty key, then a duplicate; delete both */
    prepareEmpty(&key);
    duplicateNormal(&key, "abc4567812345678", 16);
    checkDupe(&key, 0, 0, 0);
    checkDupe(&key, 1, "abc4567812345678", 16);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, true));
    REQUIRE((ham_u8_t)0 == key.get_flags());
    REQUIRE((ham_u64_t)0 == key.get_ptr());

    /* insert tiny key, then a duplicate; delete both */
    prepareTiny(&key, "1234", 4);
    duplicateNormal(&key, "abc4567812345678", 16);
    checkDupe(&key, 0, "1234", 4);
    checkDupe(&key, 1, "abc4567812345678", 16);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, true));
    REQUIRE((ham_u8_t)0 == key.get_flags());
    REQUIRE((ham_u64_t)0 == key.get_ptr());

    /* insert small key, then a duplicate; delete both */
    prepareSmall(&key, "12345678");
    duplicateNormal(&key, "abc4567812345678", 16);
    checkDupe(&key, 0, "12345678", 8);
    checkDupe(&key, 1, "abc4567812345678", 16);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, true));
    REQUIRE((ham_u8_t)0 == key.get_flags());
    REQUIRE((ham_u64_t)0 == key.get_ptr());

    /* insert normal key, then a duplicate; delete both */
    prepareNormal(&key, "1234123456785678", 16);
    duplicateNormal(&key, "abc4567812345678", 16);
    checkDupe(&key, 0, "1234123456785678", 16);
    checkDupe(&key, 1, "abc4567812345678", 16);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, true));
    REQUIRE((ham_u8_t)0 == key.get_flags());
    REQUIRE((ham_u64_t)0 == key.get_ptr());
  }

  void eraseAllDuplicateRecordTest() {
    PBtreeKey key;

    /* insert empty key, then a duplicate; delete both at once */
    prepareEmpty(&key);
    duplicateNormal(&key, "abc4567812345678", 16);
    checkDupe(&key, 0, 0, 0);
    checkDupe(&key, 1, "abc4567812345678", 16);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, false));
    REQUIRE((ham_u8_t)PBtreeKey::KEY_HAS_DUPLICATES == key.get_flags());
    checkDupe(&key, 0, "abc4567812345678", 16);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, false));
    REQUIRE((ham_u8_t)0 == key.get_flags());
    REQUIRE((ham_u64_t)0 == key.get_ptr());

    /* insert tiny key, then a duplicate; delete both at once */
    prepareTiny(&key, "1234", 4);
    duplicateNormal(&key, "abc4567812345678", 16);
    checkDupe(&key, 0, "1234", 4);
    checkDupe(&key, 1, "abc4567812345678", 16);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 1, false));
    REQUIRE((ham_u8_t)PBtreeKey::KEY_HAS_DUPLICATES == key.get_flags());
    checkDupe(&key, 0, "1234", 4);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, false));
    REQUIRE((ham_u8_t)0 == key.get_flags());
    REQUIRE((ham_u64_t)0 == key.get_ptr());

    /* insert small key, then a duplicate; delete both at once */
    prepareSmall(&key, "12345678");
    duplicateNormal(&key, "abc4567812345678", 16);
    checkDupe(&key, 0, "12345678", 8);
    checkDupe(&key, 1, "abc4567812345678", 16);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, false));
    REQUIRE((ham_u8_t)PBtreeKey::KEY_HAS_DUPLICATES == key.get_flags());
    checkDupe(&key, 0, "abc4567812345678", 16);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, false));
    REQUIRE((ham_u8_t)0 == key.get_flags());
    REQUIRE((ham_u64_t)0 == key.get_ptr());

    /* insert normal key, then a duplicate; delete both at once */
    prepareNormal(&key, "1234123456785678", 16);
    duplicateNormal(&key, "abc4567812345678", 16);
    checkDupe(&key, 0, "1234123456785678", 16);
    checkDupe(&key, 1, "abc4567812345678", 16);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 1, false));
    REQUIRE((ham_u8_t)PBtreeKey::KEY_HAS_DUPLICATES == key.get_flags());
    checkDupe(&key, 0, "1234123456785678", 16);
    REQUIRE(0 == key.erase_record(m_dbp, 0, 0, false));
    REQUIRE((ham_u8_t)0 == key.get_flags());
    REQUIRE((ham_u64_t)0 == key.get_ptr());
  }
};

TEST_CASE("BtreeKey/copyKeyInt2PubFullTest", "")
{
  BtreeKeyFixture f;
  f.structureTest();
}

TEST_CASE("BtreeKey/extendedRid", "")
{
  BtreeKeyFixture f;
  f.extendedRidTest();
}

TEST_CASE("BtreeKey/endian", "")
{
  BtreeKeyFixture f;
  f.endianTest();
}

TEST_CASE("BtreeKey/getSetExtendedKey", "")
{
  BtreeKeyFixture f;
  f.getSetExtendedKeyTest();
}

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
