/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "../src/config.h"

#include <stdexcept>
#include <cstring>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/btree.h"
#include "../src/btree_key.h"
#include "../src/util.h"
#include "../src/page.h"
#include "../src/env.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
namespace ham {

class KeyTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    KeyTest()
    :   hamsterDB_fixture("KeyTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(KeyTest, structureTest);
        BFC_REGISTER_TEST(KeyTest, extendedRidTest);
        BFC_REGISTER_TEST(KeyTest, endianTest);
        BFC_REGISTER_TEST(KeyTest, getSetExtendedKeyTest);
        BFC_REGISTER_TEST(KeyTest, setRecordTest);
        BFC_REGISTER_TEST(KeyTest, overwriteRecordTest);
        BFC_REGISTER_TEST(KeyTest, duplicateRecordTest);
        BFC_REGISTER_TEST(KeyTest, eraseRecordTest);
        BFC_REGISTER_TEST(KeyTest, eraseDuplicateRecordTest);
        BFC_REGISTER_TEST(KeyTest, eraseAllDuplicateRecordTest);
    }

protected:
    ham_db_t *m_db;
    Database *m_dbp;
    ham_env_t *m_env;

public:
    virtual void setup()
    {
        __super::setup();

        os::unlink(BFC_OPATH(".test"));

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, ham_create(m_db, BFC_OPATH(".test"), 0, 0644));

        m_dbp=(Database *)m_db;
        m_env=ham_get_env(m_db);
    }

    virtual void teardown()
    {
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
    }

    void structureTest(void)
    {
        Page *page=new Page((Environment *)m_env);
        BFC_ASSERT(page!=0);
        BFC_ASSERT_EQUAL(0, page->allocate());
        btree_node_t *node=page_get_btree_node(page);
        ::memset(node, 0, ((Environment *)m_env)->get_usable_pagesize());

        btree_key_t *key=btree_node_get_key(m_dbp, node, 0);
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(key));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(key));
        BFC_ASSERT_EQUAL((ham_u8_t)'\0', *key_get_key(key));

        key_set_ptr(key, (ham_offset_t)0x12345);
        BFC_ASSERT_EQUAL((ham_offset_t)0x12345, key_get_ptr(key));

        key_set_flags(key, (ham_u8_t)0x13);
        BFC_ASSERT_EQUAL((ham_u8_t)0x13, key_get_flags(key));

        BFC_ASSERT_EQUAL(0, page->free());
        delete page;
    }

    void extendedRidTest(void)
    {
        Page *page=new Page((Environment *)m_env);
        BFC_ASSERT(page!=0);
        BFC_ASSERT_EQUAL(0, page->allocate());
        btree_node_t *node=page_get_btree_node(page);
        ::memset(node, 0, ((Environment *)m_env)->get_usable_pagesize());

        ham_offset_t blobid;

        btree_key_t *key=btree_node_get_key(m_dbp, node, 0);
        blobid=key_get_extended_rid(m_dbp, key);
        BFC_ASSERT_EQUAL((ham_offset_t)0, blobid);

        key_set_extended_rid(m_dbp, key, (ham_offset_t)0xbaadbeef);
        blobid=ham::key_get_extended_rid(m_dbp, key);
        BFC_ASSERT_EQUAL((ham_offset_t)0xbaadbeef, blobid);

        BFC_ASSERT_EQUAL(0, page->free());
        delete page;
    }

    void endianTest(void)
    {
        ham_u8_t buffer[64]={
                0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
                0x00, 0x00, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        };

        btree_key_t *key=(btree_key_t *)&buffer[0];

        BFC_ASSERT_EQUAL((ham_offset_t)0x0123456789abcdefull,
                key_get_ptr(key));
        BFC_ASSERT_EQUAL((ham_u8_t)0xf0, key_get_flags(key));
        BFC_ASSERT_EQUAL((ham_offset_t)0xfedcba9876543210ull,
                key_get_extended_rid(m_dbp, key));
    }

    void getSetExtendedKeyTest(void)
    {
        char buffer[32];
        btree_key_t *key=(btree_key_t *)buffer;
        memset(buffer, 0, sizeof(buffer));

        key_set_extended_rid(m_dbp, key, 0x12345);
        BFC_ASSERT_EQUAL((ham_offset_t)0x12345,
                key_get_extended_rid(m_dbp, key));
    }

    void insertEmpty(btree_key_t *key, ham_u32_t flags)
    {
        ham_record_t rec;

        if (!flags)
            memset(key, 0, sizeof(*key));
        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0,
                key_set_record(m_dbp, 0, key, &rec, 0, flags, 0));
        if (!(flags&HAM_DUPLICATE))
            BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(key));

        if (!(flags&HAM_DUPLICATE)) {
            BFC_ASSERT_EQUAL((ham_u8_t)KEY_BLOB_SIZE_EMPTY,
                    key_get_flags(key));
        }
        else {
            BFC_ASSERT_EQUAL((ham_u8_t)KEY_HAS_DUPLICATES,
                    key_get_flags(key));
        }
    }

    void prepareEmpty(btree_key_t *key)
    {
        insertEmpty(key, 0);
    }

    void overwriteEmpty(btree_key_t *key)
    {
        insertEmpty(key, HAM_OVERWRITE);
    }

    void duplicateEmpty(btree_key_t *key)
    {
        insertEmpty(key, HAM_DUPLICATE);
    }

    void insertTiny(btree_key_t *key, const char *data, ham_size_t size,
            ham_u32_t flags)
    {
        ham_record_t rec, rec2;

        if (!flags)
            memset(key, 0, sizeof(*key));
        memset(&rec, 0, sizeof(rec));
        memset(&rec2, 0, sizeof(rec2));
        rec.data=(void *)data;
        rec.size=size;

        BFC_ASSERT_EQUAL(0,
                key_set_record(m_dbp, 0, key, &rec, 0, flags, 0));
        if (!(flags&HAM_DUPLICATE)) {
            BFC_ASSERT_EQUAL((ham_u8_t)KEY_BLOB_SIZE_TINY,
                key_get_flags(key));
        }
        else {
            BFC_ASSERT_EQUAL((ham_u8_t)KEY_HAS_DUPLICATES,
                    key_get_flags(key));
        }

        if (!(flags&HAM_DUPLICATE)) {
            rec2._intflags=key_get_flags(key);
            rec2._rid=key_get_ptr(key);
            BFC_ASSERT_EQUAL(0, btree_read_record(m_dbp, 0, &rec2, &rec2._rid, 0));
            BFC_ASSERT_EQUAL(rec.size, rec2.size);
            BFC_ASSERT_EQUAL(0, memcmp(rec.data, rec2.data, rec.size));
        }
    }

    void prepareTiny(btree_key_t *key, const char *data, ham_size_t size)
    {
        insertTiny(key, data, size, 0);
    }

    void overwriteTiny(btree_key_t *key, const char *data, ham_size_t size)
    {
        insertTiny(key, data, size, HAM_OVERWRITE);
    }

    void duplicateTiny(btree_key_t *key, const char *data, ham_size_t size)
    {
        insertTiny(key, data, size, HAM_DUPLICATE);
    }

    void insertSmall(btree_key_t *key, const char *data, ham_u32_t flags)
    {
        ham_record_t rec, rec2;

        if (!flags)
            memset(key, 0, sizeof(*key));
        memset(&rec, 0, sizeof(rec));
        memset(&rec2, 0, sizeof(rec2));
        rec.data=(void *)data;
        rec.size=sizeof(ham_offset_t);

        BFC_ASSERT_EQUAL(0,
                key_set_record(m_dbp, 0, key, &rec, 0, flags, 0));
        if (!(flags&HAM_DUPLICATE)) {
            BFC_ASSERT_EQUAL((ham_u8_t)KEY_BLOB_SIZE_SMALL,
                key_get_flags(key));
        }
        else {
            BFC_ASSERT_EQUAL((ham_u8_t)KEY_HAS_DUPLICATES,
                    key_get_flags(key));
        }

        if (!(flags&HAM_DUPLICATE)) {
            rec2._intflags=key_get_flags(key);
            rec2._rid=key_get_ptr(key);
            BFC_ASSERT_EQUAL(0, btree_read_record(m_dbp, 0, &rec2, &rec2._rid, 0));
            BFC_ASSERT_EQUAL(rec.size, rec2.size);
            BFC_ASSERT_EQUAL(0, memcmp(rec.data, rec2.data, rec.size));
        }
    }

    void prepareSmall(btree_key_t *key, const char *data)
    {
        insertSmall(key, data, 0);
    }

    void overwriteSmall(btree_key_t *key, const char *data)
    {
        insertSmall(key, data, HAM_OVERWRITE);
    }

    void duplicateSmall(btree_key_t *key, const char *data)
    {
        insertSmall(key, data, HAM_DUPLICATE);
    }

    void insertNormal(btree_key_t *key, const char *data, ham_size_t size,
            ham_u32_t flags)
    {
        ham_record_t rec, rec2;

        if (!flags)
            memset(key, 0, sizeof(*key));
        memset(&rec, 0, sizeof(rec));
        memset(&rec2, 0, sizeof(rec2));
        rec.data=(void *)data;
        rec.size=size;

        BFC_ASSERT_EQUAL(0,
                key_set_record(m_dbp, 0, key, &rec, 0, flags, 0));
        if (flags&HAM_DUPLICATE)
            BFC_ASSERT_EQUAL((ham_u8_t)KEY_HAS_DUPLICATES,
                    key_get_flags(key));

        if (!(flags&HAM_DUPLICATE)) {
            rec2._intflags=key_get_flags(key);
            rec2._rid=key_get_ptr(key);
            BFC_ASSERT_EQUAL(0, btree_read_record(m_dbp, 0, &rec2, &rec2._rid, 0));
            BFC_ASSERT_EQUAL(rec.size, rec2.size);
            BFC_ASSERT_EQUAL(0, memcmp(rec.data, rec2.data, rec.size));
        }
    }

    void prepareNormal(btree_key_t *key, const char *data, ham_size_t size)
    {
        insertNormal(key, data, size, 0);
    }

    void overwriteNormal(btree_key_t *key, const char *data, ham_size_t size)
    {
        insertNormal(key, data, size, HAM_OVERWRITE);
    }

    void duplicateNormal(btree_key_t *key, const char *data, ham_size_t size)
    {
        insertNormal(key, data, size, HAM_DUPLICATE);
    }

    void setRecordTest(void)
    {
        btree_key_t key;

        /* set empty record */
        prepareEmpty(&key);

        /* set tiny record */
        prepareTiny(&key, "1234", 4);

        /* set small record */
        prepareSmall(&key, "12345678");

        /* set normal record */
        prepareNormal(&key, "1234567812345678", sizeof(ham_offset_t)*2);
    }

    void overwriteRecordTest(void)
    {
        btree_key_t key;

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

    void checkDupe(btree_key_t *key, int position,
            const char *data, ham_size_t size)
    {
        BFC_ASSERT_EQUAL((ham_u8_t)KEY_HAS_DUPLICATES, key_get_flags(key));

        dupe_entry_t entry;
        DuplicateManager *dm = ((Environment *)m_env)->get_duplicate_manager();
        BFC_ASSERT_EQUAL(0,
                    dm->get(key_get_ptr(key), (ham_size_t)position, &entry));

        ham_record_t rec;
        memset(&rec, 0, sizeof(rec));

        rec._intflags=dupe_entry_get_flags(&entry);
        rec._rid=dupe_entry_get_rid(&entry);
        BFC_ASSERT_EQUAL(0, btree_read_record(m_dbp, 0, &rec, &rec._rid, 0));
        BFC_ASSERT_EQUAL(rec.size, size);
        if (size) {
            BFC_ASSERT_EQUAL(0, memcmp(rec.data, data, rec.size));
        }
        else {
            BFC_ASSERT_EQUAL((void *)0, rec.data);
        }
    }

    void duplicateRecordTest(void)
    {
        btree_key_t key;

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

    void eraseRecordTest(void)
    {
        btree_key_t key;

        /* insert empty key, then delete it */
        prepareEmpty(&key);
        BFC_ASSERT_EQUAL(0, key_erase_record(m_dbp, 0, &key, 0, 0));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(&key));
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(&key));

        /* insert tiny key, then delete it */
        prepareTiny(&key, "1234", 4);
        BFC_ASSERT_EQUAL(0, key_erase_record(m_dbp, 0, &key, 0, 0));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(&key));
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(&key));

        /* insert small key, then delete it */
        prepareSmall(&key, "12345678");
        BFC_ASSERT_EQUAL(0, key_erase_record(m_dbp, 0, &key, 0, 0));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(&key));
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(&key));

        /* insert normal key, then delete it */
        prepareNormal(&key, "1234123456785678", 16);
        BFC_ASSERT_EQUAL(0, key_erase_record(m_dbp, 0, &key, 0, 0));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(&key));
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(&key));
    }

    void eraseDuplicateRecordTest(void)
    {
        btree_key_t key;

        /* insert empty key, then a duplicate; delete both */
        prepareEmpty(&key);
        duplicateNormal(&key, "abc4567812345678", 16);
        checkDupe(&key, 0, 0, 0);
        checkDupe(&key, 1, "abc4567812345678", 16);
        BFC_ASSERT_EQUAL(0,
                key_erase_record(m_dbp, 0, &key, 0, HAM_ERASE_ALL_DUPLICATES));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(&key));
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(&key));

        /* insert tiny key, then a duplicate; delete both */
        prepareTiny(&key, "1234", 4);
        duplicateNormal(&key, "abc4567812345678", 16);
        checkDupe(&key, 0, "1234", 4);
        checkDupe(&key, 1, "abc4567812345678", 16);
        BFC_ASSERT_EQUAL(0,
                key_erase_record(m_dbp, 0, &key, 0, HAM_ERASE_ALL_DUPLICATES));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(&key));
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(&key));

        /* insert small key, then a duplicate; delete both */
        prepareSmall(&key, "12345678");
        duplicateNormal(&key, "abc4567812345678", 16);
        checkDupe(&key, 0, "12345678", 8);
        checkDupe(&key, 1, "abc4567812345678", 16);
        BFC_ASSERT_EQUAL(0,
                key_erase_record(m_dbp, 0, &key, 0, HAM_ERASE_ALL_DUPLICATES));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(&key));
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(&key));

        /* insert normal key, then a duplicate; delete both */
        prepareNormal(&key, "1234123456785678", 16);
        duplicateNormal(&key, "abc4567812345678", 16);
        checkDupe(&key, 0, "1234123456785678", 16);
        checkDupe(&key, 1, "abc4567812345678", 16);
        BFC_ASSERT_EQUAL(0,
                key_erase_record(m_dbp, 0, &key, 0, HAM_ERASE_ALL_DUPLICATES));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(&key));
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(&key));
    }

    void eraseAllDuplicateRecordTest(void)
    {
        btree_key_t key;

        /* insert empty key, then a duplicate; delete both at once */
        prepareEmpty(&key);
        duplicateNormal(&key, "abc4567812345678", 16);
        checkDupe(&key, 0, 0, 0);
        checkDupe(&key, 1, "abc4567812345678", 16);
        BFC_ASSERT_EQUAL(0, key_erase_record(m_dbp, 0, &key, 0, 0));
        BFC_ASSERT_EQUAL((ham_u8_t)KEY_HAS_DUPLICATES, key_get_flags(&key));
        checkDupe(&key, 0, "abc4567812345678", 16);
        BFC_ASSERT_EQUAL(0, key_erase_record(m_dbp, 0, &key, 0, 0));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(&key));
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(&key));

        /* insert tiny key, then a duplicate; delete both at once */
        prepareTiny(&key, "1234", 4);
        duplicateNormal(&key, "abc4567812345678", 16);
        checkDupe(&key, 0, "1234", 4);
        checkDupe(&key, 1, "abc4567812345678", 16);
        BFC_ASSERT_EQUAL(0, key_erase_record(m_dbp, 0, &key, 1, 0));
        BFC_ASSERT_EQUAL((ham_u8_t)KEY_HAS_DUPLICATES, key_get_flags(&key));
        checkDupe(&key, 0, "1234", 4);
        BFC_ASSERT_EQUAL(0, key_erase_record(m_dbp, 0, &key, 0, 0));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(&key));
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(&key));

        /* insert small key, then a duplicate; delete both at once */
        prepareSmall(&key, "12345678");
        duplicateNormal(&key, "abc4567812345678", 16);
        checkDupe(&key, 0, "12345678", 8);
        checkDupe(&key, 1, "abc4567812345678", 16);
        BFC_ASSERT_EQUAL(0, key_erase_record(m_dbp, 0, &key, 0, 0));
        BFC_ASSERT_EQUAL((ham_u8_t)KEY_HAS_DUPLICATES, key_get_flags(&key));
        checkDupe(&key, 0, "abc4567812345678", 16);
        BFC_ASSERT_EQUAL(0, key_erase_record(m_dbp, 0, &key, 0, 0));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(&key));
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(&key));

        /* insert normal key, then a duplicate; delete both at once */
        prepareNormal(&key, "1234123456785678", 16);
        duplicateNormal(&key, "abc4567812345678", 16);
        checkDupe(&key, 0, "1234123456785678", 16);
        checkDupe(&key, 1, "abc4567812345678", 16);
        BFC_ASSERT_EQUAL(0, key_erase_record(m_dbp, 0, &key, 1, 0));
        BFC_ASSERT_EQUAL((ham_u8_t)KEY_HAS_DUPLICATES, key_get_flags(&key));
        checkDupe(&key, 0, "1234123456785678", 16);
        BFC_ASSERT_EQUAL(0, key_erase_record(m_dbp, 0, &key, 0, 0));
        BFC_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(&key));
        BFC_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(&key));
    }

};

} // namespace ham

BFC_REGISTER_FIXTURE(KeyTest);

