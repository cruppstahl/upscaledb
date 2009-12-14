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
#include <time.h>
#include <ham/hamsterdb.h>
#include "memtracker.h"
#include "../src/db.h"
#include "../src/version.h"
#include "../src/serial.h"
#include "../src/btree.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

static int HAM_CALLCONV 
my_compare_func(ham_db_t *db, 
                const ham_u8_t *lhs, ham_size_t lhs_length,
                const ham_u8_t *rhs, ham_size_t rhs_length)
{
    (void)lhs;
    (void)rhs;
    (void)lhs_length;
    (void)rhs_length;
    return (0);
}

static int HAM_CALLCONV 
my_prefix_compare_func(ham_db_t *db, 
               const ham_u8_t *lhs, ham_size_t lhs_length,
               ham_size_t lhs_real_length,
               const ham_u8_t *rhs, ham_size_t rhs_length,
               ham_size_t rhs_real_length)
{
    (void)lhs;
    (void)rhs;
    (void)lhs_length;
    (void)rhs_length;
    (void)lhs_real_length;
    (void)rhs_real_length;
    return (0);
}

class HamsterdbTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    HamsterdbTest()
    :   hamsterDB_fixture("HamsterdbTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(HamsterdbTest, versionTest);
        BFC_REGISTER_TEST(HamsterdbTest, licenseTest);
        BFC_REGISTER_TEST(HamsterdbTest, newTest);
        BFC_REGISTER_TEST(HamsterdbTest, deleteTest);
        BFC_REGISTER_TEST(HamsterdbTest, openTest);
        BFC_REGISTER_TEST(HamsterdbTest, invHeaderTest);
        BFC_REGISTER_TEST(HamsterdbTest, invVersionTest);
        BFC_REGISTER_TEST(HamsterdbTest, createTest);
        BFC_REGISTER_TEST(HamsterdbTest, createPagesizeTest);
        BFC_REGISTER_TEST(HamsterdbTest, createMaxkeysTooHighTest);
        BFC_REGISTER_TEST(HamsterdbTest, createCloseCreateTest);
        BFC_REGISTER_TEST(HamsterdbTest, createPagesizeReopenTest);
        BFC_REGISTER_TEST(HamsterdbTest, readOnlyTest);
        BFC_REGISTER_TEST(HamsterdbTest, invalidPagesizeTest);
        BFC_REGISTER_TEST(HamsterdbTest, invalidDamInEnvTest);
        BFC_REGISTER_TEST(HamsterdbTest, setPre110DamTest);
        BFC_REGISTER_TEST(HamsterdbTest, recnoUsesSequentialDamTest);
        BFC_REGISTER_TEST(HamsterdbTest, unknownDamTest);
        BFC_REGISTER_TEST(HamsterdbTest, getErrorTest);
        BFC_REGISTER_TEST(HamsterdbTest, setPrefixCompareTest);
        BFC_REGISTER_TEST(HamsterdbTest, setCompareTest);
        BFC_REGISTER_TEST(HamsterdbTest, findTest);
        BFC_REGISTER_TEST(HamsterdbTest, findEmptyRecordTest);
        BFC_REGISTER_TEST(HamsterdbTest, nearFindTest);
        BFC_REGISTER_TEST(HamsterdbTest, nearFindStressTest);
        BFC_REGISTER_TEST(HamsterdbTest, insertTest);
        BFC_REGISTER_TEST(HamsterdbTest, insertBigKeyTest);
        BFC_REGISTER_TEST(HamsterdbTest, eraseTest);
        BFC_REGISTER_TEST(HamsterdbTest, flushTest);
        BFC_REGISTER_TEST(HamsterdbTest, flushBackendTest);
        BFC_REGISTER_TEST(HamsterdbTest, closeTest);
        BFC_REGISTER_TEST(HamsterdbTest, closeWithCursorsTest);
        BFC_REGISTER_TEST(HamsterdbTest, closeWithCursorsAutoCleanupTest);
        BFC_REGISTER_TEST(HamsterdbTest, compareTest);
        BFC_REGISTER_TEST(HamsterdbTest, prefixCompareTest);
        BFC_REGISTER_TEST(HamsterdbTest, cursorCreateTest);
        BFC_REGISTER_TEST(HamsterdbTest, cursorCloneTest);
        BFC_REGISTER_TEST(HamsterdbTest, cursorMoveTest);
        BFC_REGISTER_TEST(HamsterdbTest, cursorReplaceTest);
        BFC_REGISTER_TEST(HamsterdbTest, cursorFindTest);
        BFC_REGISTER_TEST(HamsterdbTest, cursorInsertTest);
        BFC_REGISTER_TEST(HamsterdbTest, cursorEraseTest);
        BFC_REGISTER_TEST(HamsterdbTest, cursorCloseTest);
        BFC_REGISTER_TEST(HamsterdbTest, cursorGetErasedItemTest);
        BFC_REGISTER_TEST(HamsterdbTest, replaceKeyTest);
        BFC_REGISTER_TEST(HamsterdbTest, replaceKeyFileTest);
        BFC_REGISTER_TEST(HamsterdbTest, callocTest);
        BFC_REGISTER_TEST(HamsterdbTest, strerrorTest);
        BFC_REGISTER_TEST(HamsterdbTest, contextDataTest);
        BFC_REGISTER_TEST(HamsterdbTest, recoveryTest);
        BFC_REGISTER_TEST(HamsterdbTest, recoveryNegativeTest);
        BFC_REGISTER_TEST(HamsterdbTest, recoveryEnvTest);
        BFC_REGISTER_TEST(HamsterdbTest, recoveryEnvNegativeTest);
        BFC_REGISTER_TEST(HamsterdbTest, btreeMacroTest);
        BFC_REGISTER_TEST(HamsterdbTest, cursorInsertAppendTest);
        BFC_REGISTER_TEST(HamsterdbTest, negativeCursorInsertAppendTest);
        BFC_REGISTER_TEST(HamsterdbTest, recordCountTest);
    }

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    virtual void setup() 
    { 
        __super::setup();

        os::unlink(BFC_OPATH(".test"));
        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL(0, ham_create(m_db, 0, HAM_IN_MEMORY_DB, 0));
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void versionTest(void)
    {
        ham_u32_t major, minor, revision;

        ham_get_version(0, 0, 0);
        ham_get_version(&major, &minor, &revision);

        BFC_ASSERT_EQUAL((ham_u32_t)HAM_VERSION_MAJ, major);
        BFC_ASSERT_EQUAL((ham_u32_t)HAM_VERSION_MIN, minor);
        BFC_ASSERT_EQUAL((ham_u32_t)HAM_VERSION_REV, revision);
    };

    void licenseTest(void)
    {
        const char *licensee=0, *product=0;

        ham_get_license(0, 0);
        ham_get_license(&licensee, &product);

        BFC_ASSERT_EQUAL(0, strcmp(HAM_LICENSEE, licensee));
        BFC_ASSERT_EQUAL(0, strcmp(HAM_PRODUCT_NAME, product));
    };

    void newTest(void)
    {
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, ham_new(0));
    }

    void deleteTest(void)
    {
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, ham_delete(0));
    }

    void openTest(void)
    {
        ham_db_t *db;
        ham_parameter_t params[]={
            { 0x1234567, 0 },
            { 0, 0 }
        };

        BFC_ASSERT_EQUAL(0, ham_new(&db));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open(0, "test.db", 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open(db, 0, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open(db, 0, HAM_IN_MEMORY_DB));
        BFC_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, 
                ham_open(db, "xxxx...", 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open(db, "test.db", HAM_IN_MEMORY_DB));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open(db, "test.db", HAM_ENABLE_DUPLICATES));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open_ex(db, "test.db", HAM_ENABLE_DUPLICATES, params));

#if WIN32
        BFC_ASSERT_EQUAL(HAM_IO_ERROR, 
                ham_open(db, "c:\\windows", 0));
#else
        BFC_ASSERT_EQUAL(HAM_IO_ERROR, 
                ham_open(db, "/usr", 0));
#endif

        ham_delete(db);
    }

    void invHeaderTest(void)
    {
        ham_db_t *db;

        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(HAM_INV_FILE_HEADER, 
                ham_open(db, BFC_IPATH("data/inv-file-header.hdb"), 0));

        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        ham_delete(db);
    }

    void invVersionTest(void)
    {
        ham_db_t *db;

        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(HAM_INV_FILE_VERSION, 
                ham_open(db, BFC_IPATH("data/inv-file-version.hdb"), 0));

        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        ham_delete(db);
    }

    void createTest(void)
    {
        ham_db_t *db;
        ham_parameter_t cs[]={{HAM_PARAM_CACHESIZE, 1024}, {0, 0}};
        ham_parameter_t ps[]={{HAM_PARAM_PAGESIZE,   512}, {0, 0}};

        BFC_ASSERT_EQUAL(0, ham_new(&db));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(0, ".test.db", 0, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(db, 0, 0, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(db, 0, HAM_IN_MEMORY_DB|HAM_CACHE_STRICT, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(db, ".test.db", 
                    HAM_CACHE_UNLIMITED|HAM_CACHE_STRICT, 0644));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create_ex(db, ".test.db", HAM_CACHE_UNLIMITED, 0, &cs[0]));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open(db, ".test.db", HAM_CACHE_UNLIMITED|HAM_CACHE_STRICT));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open_ex(db, ".test.db", HAM_CACHE_UNLIMITED, &cs[0]));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create_ex(db, 0, HAM_IN_MEMORY_DB, 0, &cs[0]));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create_ex(db, 0, HAM_IN_MEMORY_DB|HAM_READ_ONLY, 0, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create_ex(db, 0, HAM_READ_ONLY, 0, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PAGESIZE, 
                ham_create_ex(db, BFC_OPATH(".test"), 0, 0, &ps[0]));
#if WIN32
        BFC_ASSERT_EQUAL(HAM_IO_ERROR, 
                ham_create(db, "c:\\windows", 0, 0664));
#else
        BFC_ASSERT_EQUAL(HAM_IO_ERROR, 
                ham_create(db, "/home", 0, 0664));
#endif
        ham_delete(db);
    }

    void createPagesizeTest(void)
    {
        ham_db_t *db;

        BFC_ASSERT_EQUAL(0, ham_new(&db));

        ham_parameter_t ps[]={{HAM_PARAM_PAGESIZE,   512}, {0, 0}};

        BFC_ASSERT_EQUAL(HAM_INV_PAGESIZE, 
                ham_create_ex(db, BFC_OPATH(".test"), 0, 0644, &ps[0]));

        ps[0].value=1024;
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(db, BFC_OPATH(".test"), 0, 0644, &ps[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));

        ham_delete(db);
    }

    void createMaxkeysTooHighTest(void)
    {
        ham_db_t *db;

        BFC_ASSERT_EQUAL(0, ham_new(&db));

        ham_parameter_t ps[]={{HAM_PARAM_PAGESIZE, 1024*1024*128}, 
                              {HAM_PARAM_KEYSIZE, 16}, 
                              {0, 0}};

        BFC_ASSERT_EQUAL(HAM_INV_KEYSIZE, 
                ham_create_ex(db, BFC_OPATH(".test"), 0, 0644, &ps[0]));

        ham_delete(db);
    }

    void createCloseCreateTest(void)
    {
        ham_db_t *db;

        BFC_ASSERT_EQUAL(0, ham_new(&db));

        BFC_ASSERT_EQUAL(0, ham_create(db, BFC_OPATH(".test"), 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(db, BFC_OPATH(".test"), 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));

        ham_delete(db);
    }

    void createPagesizeReopenTest(void)
    {
        ham_db_t *db;
        ham_parameter_t ps[]={{HAM_PARAM_PAGESIZE,   1024*128}, {0, 0}};

        BFC_ASSERT_EQUAL(0, ham_new(&db));

        BFC_ASSERT_EQUAL(0, ham_create_ex(db, BFC_OPATH(".test"), 0, 0664, &ps[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(db, BFC_OPATH(".test"), 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
    }

    void readOnlyTest(void)
    {
        ham_db_t *db;
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *cursor;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0, ham_new(&db));

        BFC_ASSERT_EQUAL(0, ham_create(db, BFC_OPATH(".test"), 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(db, BFC_OPATH(".test"), HAM_READ_ONLY));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));

        BFC_ASSERT_EQUAL(HAM_DB_READ_ONLY, 
                ham_insert(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_DB_READ_ONLY, 
                ham_erase(db, 0, &key, 0));
        BFC_ASSERT_EQUAL(HAM_DB_READ_ONLY, 
                ham_cursor_overwrite(cursor, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_DB_READ_ONLY, 
                ham_cursor_insert(cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_DB_READ_ONLY, 
                ham_cursor_erase(cursor, 0));

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
    }

    void invalidPagesizeTest(void)
    {
        ham_db_t *db;
        ham_parameter_t p[]={
            {HAM_PARAM_PAGESIZE, 1024}, 
            {HAM_PARAM_KEYSIZE,   512}, 
            {0, 0}
        };

        BFC_ASSERT_EQUAL(0, ham_new(&db));

        BFC_ASSERT_EQUAL(HAM_INV_KEYSIZE, 
                ham_create_ex(db, BFC_OPATH(".test"), 0, 0664, &p[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));

        p[1].value=15;

        // only pagesize of 1k, 2k, multiples of 2k are allowed
        p[0].value=1024;
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(db, BFC_OPATH(".test"), 0, 0664, &p[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        p[0].value=2048;
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(db, BFC_OPATH(".test"), 0, 0664, &p[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        p[0].value=4096;
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(db, BFC_OPATH(".test"), 0, 0664, &p[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        p[0].value=1024*3;
        BFC_ASSERT_EQUAL(HAM_INV_PAGESIZE, 
                ham_create_ex(db, BFC_OPATH(".test"), 0, 0664, &p[0]));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
    }

    void invalidDamInEnvTest(void)
    {
        ham_env_t *env;
        ham_parameter_t p[]={
            {HAM_PARAM_DATA_ACCESS_MODE, HAM_DAM_RANDOM_WRITE}, 
            {0, 0}
        };

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_create_ex(env, BFC_OPATH(".test"), 0, 0664, &p[0]));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_open_ex(env, BFC_OPATH(".test"), 0, &p[0]));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void setPre110DamTest(void)
    {
        ham_db_t *db;
        ham_parameter_t p[]={
            {HAM_PARAM_DATA_ACCESS_MODE, HAM_DAM_ENFORCE_PRE110_FORMAT}, 
            {0, 0}
        };

        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, 
                ham_open(db, 
                    BFC_IPATH("data/recno-endian-test-open-database-be.hdb"),
                        0));
        BFC_ASSERT(HAM_DAM_ENFORCE_PRE110_FORMAT&db_get_data_access_mode(db));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create_ex(db, BFC_OPATH(".test"), 0, 0664, &p[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open_ex(db, BFC_OPATH(".test"), 0, &p[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        ham_delete(db);
    }

    void recnoUsesSequentialDamTest(void)
    {
        ham_db_t *db;

        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, 
                ham_create(db, BFC_OPATH(".test"), HAM_RECORD_NUMBER, 0664));
        BFC_ASSERT(HAM_DAM_SEQUENTIAL_INSERT&db_get_data_access_mode(db));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));

        BFC_ASSERT_EQUAL(0, 
                ham_open(db, BFC_OPATH(".test"), 0));
        BFC_ASSERT(HAM_DAM_SEQUENTIAL_INSERT&db_get_data_access_mode(db));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        ham_delete(db);
    }

    void unknownDamTest(void)
    {
        ham_db_t *db;
        ham_parameter_t p[]={
            {HAM_PARAM_DATA_ACCESS_MODE, 99}, 
            {0, 0}
        };

        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create_ex(db, BFC_OPATH(".test"), 0, 0664, &p[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open_ex(db, BFC_OPATH(".test"), 0, &p[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        ham_delete(db);
    }

    void getErrorTest(void)
    {
        BFC_ASSERT_EQUAL(0, ham_get_error(0));
        BFC_ASSERT_EQUAL(0, ham_get_error(m_db));
    }

    void setPrefixCompareTest(void)
    {
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_set_prefix_compare_func(0, 0));
    }

    void setCompareTest(void)
    {
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_set_compare_func(0, 0));
    }

    void findTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_find(0, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_find(m_db, 0, 0, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_find(m_db, 0, &key, 0, 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(m_db, 0, &key, &rec, 0));
    }

    void findEmptyRecordTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, 0));

        ham_cursor_t *cursor;
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_create(m_db, 0, 0, &cursor));

        rec.data=(void *)"123";
        rec.size=12345;
        rec.flags=HAM_RECORD_USER_ALLOC;
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));

        BFC_ASSERT_EQUAL(0, key.size);
        BFC_ASSERT_EQUAL(0, key.data);
        BFC_ASSERT_EQUAL(0, rec.size);
        BFC_ASSERT_EQUAL(0, rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    }


static int HAM_CALLCONV my_prefix_compare_func_u32(ham_db_t *db, 
                                  const ham_u8_t *lhs, ham_size_t lhs_length, 
                                  ham_size_t lhs_real_length,
                                  const ham_u8_t *rhs, ham_size_t rhs_length,
                                  ham_size_t rhs_real_length)
{
    ham_s32_t *l = (ham_s32_t *)lhs;
    ham_s32_t *r = (ham_s32_t *)rhs;
    ham_size_t len = (lhs_length < rhs_length ? lhs_length : rhs_length);

    ham_assert(lhs, (0));
    ham_assert(rhs, (0));

    len /= 4;
    while (len > 0)
    {
        if (*l < *r)
            return -1;
        else if (*l > *r)
            return +1;
        len--;
        l++;
        r++;
    }
    return HAM_PREFIX_REQUEST_FULLKEY;
}


static int HAM_CALLCONV my_compare_func_u32(ham_db_t *db, 
                                  const ham_u8_t *lhs, ham_size_t lhs_length, 
                                  const ham_u8_t *rhs, ham_size_t rhs_length)
{
    ham_s32_t *l = (ham_s32_t *)lhs;
    ham_s32_t *r = (ham_s32_t *)rhs;
    ham_size_t len = (lhs_length < rhs_length ? lhs_length : rhs_length);

    ham_assert(lhs, (0));
    ham_assert(rhs, (0));

    len /= 4;
    while (len > 0)
    {
        if (*l < *r)
            return -1;
        else if (*l > *r)
            return +1;
        len--;
        l++;
        r++;
    }
    if (lhs_length<rhs_length)
    {
        return (-1);
    }
    else if (rhs_length<lhs_length) 
    {
        return (+1);
    }
    return 0;
}

    void nearFindStressTest(void)
    {
        const int RECORD_COUNT_PER_DB = 200000;
        ham_env_t *env;
        ham_db_t *db;
        time_t t[5];
        struct my_key_t
        {
            ham_s32_t val1;
            ham_u32_t val2;
            ham_u32_t val3;
            ham_u32_t val4;
        };
        struct my_rec_t
        {
            ham_s32_t val1;
            ham_u32_t val2[15];
        };
        ham_parameter_t ps[]={
            {HAM_PARAM_PAGESIZE,   2*64*1024}, /* UNIX == WIN now */
            {HAM_PARAM_CACHESIZE,    32},
            {0, 0}
        };
        ham_parameter_t ps2[]={
            {HAM_PARAM_KEYSIZE,    sizeof(my_key_t)},
            {0, 0}
        };

        ham_key_t key;
        ham_record_t rec;

        my_key_t my_key;
        my_rec_t my_rec;
        
        time(&t[0]);

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_ex(env, BFC_OPATH(".test"), 
                    HAM_DISABLE_MMAP, 0644, ps));
        
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        ham_size_t keycount = 0;
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 1, 0, ps2));
#ifdef HAM_ENABLE_INTERNAL
        BFC_ASSERT_EQUAL(0, 
                ham_calc_maxkeys_per_page(db, &keycount, sizeof(my_key)));
        BFC_ASSERT_EQUAL(/* 2424 */ 4852, keycount);
#else
        BFC_ASSERT_EQUAL(HAM_NOT_IMPLEMENTED, 
                ham_calc_maxkeys_per_page(db, &keycount, sizeof(my_key)));
#endif
        keycount = 4852; /* for 128K pagesize */
        BFC_ASSERT_EQUAL(0, 
                ham_set_prefix_compare_func(db, &my_prefix_compare_func_u32));
        BFC_ASSERT_EQUAL(0, 
                ham_set_compare_func(db, &my_compare_func_u32));
        
        std::cerr << "1K inserts: ";

        /* insert the records: key=2*i; rec=100*i */
        int lower_bound_of_range = 0;
        int upper_bound_of_range = (RECORD_COUNT_PER_DB - 1) * 2;
        ham_cursor_t *cursor;
        BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));
        int i;
        for (i = 0; i < RECORD_COUNT_PER_DB; i++)
        {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            ::memset(&my_key, 0, sizeof(my_key));
            ::memset(&my_rec, 0, sizeof(my_rec));

            my_rec.val1 = 100 * i; // record values thus are 50 * key values...
            rec.data = &my_rec;
            rec.size = sizeof(my_rec);
            rec.flags = HAM_RECORD_USER_ALLOC;

            my_key.val1 = 2 * i;
            key.data = (void *)&my_key;
            key.size = sizeof(my_key);
            key.flags = HAM_KEY_USER_ALLOC;

            BFC_ASSERT_EQUAL_I(0, ham_cursor_insert(cursor, &key, &rec, 0), i);

            if (i % 1000 == 999) {
                std::cerr << ".";
                if (i % 10000 == 9999 || i <= 10000)
                {
                    std::cerr << "+";
#if 0 // this completely thrashes the cache as it is quite similar to a full scan of the DB
#ifdef HAM_ENABLE_INTERNAL
                    BFC_ASSERT_EQUAL_I(0, ham_check_integrity(db, NULL), i);
#else
                    BFC_ASSERT_EQUAL_I(HAM_NOT_IMPLEMENTED, ham_check_integrity(db, NULL), i);
#endif
#endif
                }
            }
        }
        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));

        std::cerr << std::endl;

#ifdef HAM_ENABLE_INTERNAL
        BFC_ASSERT_EQUAL(0, ham_check_integrity(db, NULL));
#else
        BFC_ASSERT_EQUAL(HAM_NOT_IMPLEMENTED, ham_check_integrity(db, NULL));
#endif

        my_rec_t *r;
        my_key_t *k;

        time(&t[1]);

        std::cerr << "1K steps: ";

        /* show record collection */
        BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));
        for (i = 0; i < RECORD_COUNT_PER_DB; i++)
        {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            BFC_ASSERT_EQUAL_I(0, ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT), i);
            BFC_ASSERT_NOTEQUAL_I(key.data, 0, i);
            BFC_ASSERT_NOTEQUAL_I(rec.data, 0, i);
            r = (my_rec_t *)rec.data;
            k = (my_key_t *)key.data;
#if 0
            printf("rec: %d vs. %d, ", r->val1, 100*i);
            printf("key: %d vs. %d\n", k->val1, 2*i);
#else
            BFC_ASSERT_EQUAL(r->val1, 100*i);
            BFC_ASSERT_EQUAL(k->val1, 2*i);
#endif
            if (i % 1000 == 999) {
                std::cerr << ".";
                if (i % 10000 == 9999 || i <= 10000)
                {
                    std::cerr << "+";
#if 0
#ifdef HAM_ENABLE_INTERNAL
                    BFC_ASSERT_EQUAL_I(0, ham_check_integrity(db, NULL), i);
#else
                    BFC_ASSERT_EQUAL_I(HAM_NOT_IMPLEMENTED, ham_check_integrity(db, NULL), i);
#endif
#endif
                }
            }
        }
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));

        time(&t[2]);

#ifdef HAM_ENABLE_INTERNAL
        BFC_ASSERT_EQUAL(0, ham_check_integrity(db, NULL));
#else
        BFC_ASSERT_EQUAL(HAM_NOT_IMPLEMENTED, ham_check_integrity(db, NULL));
#endif

        std::cerr << std::endl;

        /* 
        A)

        now the real thing starts: search for records which match and don't 
        exist, using the various modes.
        Since we know the keys are all == 0 MOD 2, we know we'll have an EXACT
        hit for every second entry when we search for keys == 0 MOD 3.

        B)

        After a round of that, we do it all over again, but now while we 
        delete
        every key == 0 MOD 5 at the same time; that is: every second delete 
        should succeed, while it impacts our search hits as any records with
        key == 0 MOD 10 will be gone by the time we check them out.

        C)

        The third round is the specialties corner, where we insert additional
        records with key == 0 MOD 2 AT THE HIGH END, while searching for an
        upper and lower non-existing odd key after each insert; at least one
        of 'em should hit the firnge case of edge-of-page-block with the
        match landing on the wrong side initially, requiring the internal 
        'let's jump to the neighbouring block' code to work.

        D)

        When we get through that, we do the same at the BOTTOM side of the 
        spectrum.

        E)

        And the last part is a bit of random-access simulation, where
        we search for keys == 0 MOD 3, while we know the state of affairs
        in the store so we can predict exact match success/failure, but
        added to this, we traverse a few records up and down from the match
        using cursor_move() and check to ensure those are all in proper order.

        The random generator is a simple prime-modulo thingy, which uses a
        large random number to ensure we're nicely jumping up & down
        throughout the range.
        */

        time(&t[3]);

        std::cerr << "1K searches EQ/LT/GT/mixed: ";

        BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));
        for (i = lower_bound_of_range/2 - 7; i < upper_bound_of_range/2 + 7; i++)
        {
            int looking_for = 3 * i;

            /* determine expected values now; then do all the searches and check 'em */
            bool eq_expect; // EQ key exists?
            int le_keyval; // LE key.
            bool le_expect;
            int lt_keyval; // LT key.
            bool lt_expect;
            int ge_keyval; // GE key.
            bool ge_expect;
            int gt_keyval; // GT key.
            bool gt_expect;

            eq_expect = !(looking_for % 2); // EQ key exists?
            eq_expect &= (looking_for >= lower_bound_of_range && looking_for <= upper_bound_of_range);

            le_keyval = looking_for - abs(looking_for % 2); // LE key.
            while (le_keyval > upper_bound_of_range)
            {
                le_keyval -= 2;
            }
            le_expect = (le_keyval >= lower_bound_of_range && le_keyval <= upper_bound_of_range);

            lt_keyval = (looking_for - 1) - (abs(looking_for - 1) % 2); // LT key.
            while (lt_keyval > upper_bound_of_range)
            {
                lt_keyval -= 2;
            }
            lt_expect = (lt_keyval >= lower_bound_of_range && lt_keyval <= upper_bound_of_range);

            ge_keyval = looking_for + abs(looking_for % 2); // GE key.
            while (ge_keyval < lower_bound_of_range)
            {
                ge_keyval += 2;
            }
            ge_expect = (ge_keyval >= lower_bound_of_range && ge_keyval <= upper_bound_of_range);

            gt_keyval = (looking_for + 1) + (abs(looking_for + 1) % 2); // GT key.
            while (gt_keyval < lower_bound_of_range)
            {
                gt_keyval += 2;
            }
            gt_expect = (gt_keyval >= lower_bound_of_range && gt_keyval <= upper_bound_of_range);

#define PREP()                                        \
            ::memset(&key, 0, sizeof(key));            \
            ::memset(&rec, 0, sizeof(rec));            \
            ::memset(&my_key, 0, sizeof(my_key));    \
            ::memset(&my_rec, 0, sizeof(my_rec));    \
                                                    \
            my_key.val1 = looking_for;                \
            key.data = (void *)&my_key;                \
            key.size = sizeof(my_key);                \
            key.flags = HAM_KEY_USER_ALLOC;

            PREP();
            BFC_ASSERT_EQUAL_I((eq_expect ? 0 : HAM_KEY_NOT_FOUND), ham_cursor_find_ex(cursor, &key, &rec, 0), i);
            r = (my_rec_t *)rec.data;
            k = (my_key_t *)key.data;
            BFC_ASSERT_EQUAL_I((k ? k->val1 : 666), looking_for, i);
            BFC_ASSERT_EQUAL_I((r ? r->val1 : 666), (eq_expect ? looking_for * 50 : 666), i);

            PREP();
            BFC_ASSERT_EQUAL_I((lt_expect ? 0 : HAM_KEY_NOT_FOUND), ham_cursor_find_ex(cursor, &key, &rec, HAM_FIND_LT_MATCH), i);
            r = (my_rec_t *)rec.data;
            k = (my_key_t *)key.data;
            BFC_ASSERT_EQUAL_I((k ? k->val1 : 666), (lt_expect ? lt_keyval : looking_for), i); // key is untouched when no match found at all
            BFC_ASSERT_EQUAL_I((r ? r->val1 : 666), (lt_expect ? lt_keyval * 50 : 666), i);

            PREP();
            BFC_ASSERT_EQUAL_I((gt_expect ? 0 : HAM_KEY_NOT_FOUND), ham_cursor_find_ex(cursor, &key, &rec, HAM_FIND_GT_MATCH), i);
            r = (my_rec_t *)rec.data;
            k = (my_key_t *)key.data;
            BFC_ASSERT_EQUAL_I((k ? k->val1 : 666), (gt_expect ? gt_keyval : looking_for), i); // key is untouched when no match found at all
            BFC_ASSERT_EQUAL_I((r ? r->val1 : 666), (gt_expect ? gt_keyval * 50 : 666), i);

            PREP();
            BFC_ASSERT_EQUAL_I((le_expect ? 0 : HAM_KEY_NOT_FOUND), ham_cursor_find_ex(cursor, &key, &rec, HAM_FIND_LEQ_MATCH), i);
            r = (my_rec_t *)rec.data;
            k = (my_key_t *)key.data;
            BFC_ASSERT_EQUAL_I((k ? k->val1 : 666), (le_expect ? le_keyval : looking_for), i); // key is untouched when no match found at all
            BFC_ASSERT_EQUAL_I((r ? r->val1 : 666), (le_expect ? le_keyval * 50 : 666), i);

            PREP();
            BFC_ASSERT_EQUAL_I((ge_expect ? 0 : HAM_KEY_NOT_FOUND), ham_cursor_find_ex(cursor, &key, &rec, HAM_FIND_GEQ_MATCH), i);
            r = (my_rec_t *)rec.data;
            k = (my_key_t *)key.data;
            BFC_ASSERT_EQUAL_I((k ? k->val1 : 666), (ge_expect ? ge_keyval : looking_for), i); // key is untouched when no match found at all
            BFC_ASSERT_EQUAL_I((r ? r->val1 : 666), (ge_expect ? ge_keyval * 50 : 666), i);

            PREP();
            bool mix_expect = (le_expect || ge_expect);
            BFC_ASSERT_EQUAL_I((mix_expect ? 0 : HAM_KEY_NOT_FOUND), ham_cursor_find_ex(cursor, &key, &rec, HAM_FIND_NEAR_MATCH), i);
            r = (my_rec_t *)rec.data;
            k = (my_key_t *)key.data;
            BFC_ASSERT_I(((k ? k->val1 : 666) == le_keyval) 
                        || ((k ? k->val1 : 666) == (mix_expect ? ge_keyval : looking_for)), // key is untouched when no match found at all
                        i);
            BFC_ASSERT_I(((k ? k->val1 : 666) == le_keyval)
                ? ((r ? r->val1 : 666) == (mix_expect ? le_keyval * 50 : 666)) 
                : ((r ? r->val1 : 666) == (mix_expect ? ge_keyval * 50 : 666)), 
                i);

            PREP();
            mix_expect = (lt_expect || gt_expect);
            BFC_ASSERT_EQUAL_I((mix_expect ? 0 : HAM_KEY_NOT_FOUND), 
                            ham_cursor_find_ex(cursor, &key, &rec, (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)),
                            i);
            r = (my_rec_t *)rec.data;
            k = (my_key_t *)key.data;
            BFC_ASSERT_I(((k ? k->val1 : 666) == lt_keyval) 
                        || ((k ? k->val1 : 666) == (mix_expect ? gt_keyval : looking_for)), // key is untouched when no match found at all
                        i);
            BFC_ASSERT_I(((k ? k->val1 : 666) == lt_keyval) 
                ? ((r ? r->val1 : 666) == (mix_expect ? lt_keyval * 50 : 666)) 
                : ((r ? r->val1 : 666) == (mix_expect ? gt_keyval * 50 : 666)),
                i);

#undef PREP

            if (i % 1000 == 999) {
                std::cerr << ".";
                if (i % 10000 == 9999 || i <= 10000)
                {
                    std::cerr << "+";
                }
            }
        }
        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));

        std::cerr << std::endl;


        BFC_ASSERT_EQUAL(0, ham_close(db, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));

        time(&t[4]);

        double dt[4];
            
        dt[0] = difftime(t[1], t[0]);
        dt[1] = difftime(t[2], t[1]);
        dt[2] = difftime(t[3], t[2]);
        dt[3] = difftime(t[4], t[3]);

        std::cout << std::endl << "## stresstest timing report ##" << std::endl;

        std::cout << "INSERT " << RECORD_COUNT_PER_DB << " records @ " << RECORD_COUNT_PER_DB / (0.001 + dt[0]) << " rec/sec." << std::endl;
        std::cout << "CURSOR MOVE NEXT " << RECORD_COUNT_PER_DB << " records @ " << RECORD_COUNT_PER_DB / (0.001 + dt[1]) << " rec/sec." << std::endl;
        std::cout << "INTEGRITY CHECK " << RECORD_COUNT_PER_DB << " records @ " << RECORD_COUNT_PER_DB / (0.001 + dt[2]) << " rec/sec." << std::endl;
        std::cout << "FIND (=/</>/<=/>=/~/<~>) " << 7 * RECORD_COUNT_PER_DB << " records @ " << 7 * RECORD_COUNT_PER_DB / (0.001 + dt[3]) << " rec/sec." << std::endl;
    }

    void nearFindTest(void)
    {
        ham_db_t *db;
        ham_parameter_t ps[]={{HAM_PARAM_PAGESIZE,   64*1024}, {0, 0}};
        const int MY_KEY_SIZE = 6554;
        struct my_key_t
        {
            ham_u32_t key_val1;
            ham_u32_t key_surplus[MY_KEY_SIZE/4];
        };
        struct my_rec_t
        {
            ham_u32_t rec_val1;
            char rec_val2[512];
        };

        BFC_ASSERT_EQUAL(0, ham_new(&db));
        ham_size_t keycount = 0;
        BFC_ASSERT_EQUAL(HAM_NOT_INITIALIZED, 
                ham_calc_maxkeys_per_page(db, &keycount, MY_KEY_SIZE));
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(db, BFC_OPATH(".test"), 0, 0644, &ps[0]));
#ifdef HAM_ENABLE_INTERNAL
        BFC_ASSERT_EQUAL(0, 
                ham_calc_maxkeys_per_page(db, &keycount, MY_KEY_SIZE));
        BFC_ASSERT_EQUAL(8, keycount);
#else
        BFC_ASSERT_EQUAL(HAM_NOT_IMPLEMENTED, 
                ham_calc_maxkeys_per_page(db, &keycount, MY_KEY_SIZE));
#endif
        keycount = 8;
        BFC_ASSERT_EQUAL(0, 
                ham_set_prefix_compare_func(db, &my_prefix_compare_func_u32));
        BFC_ASSERT_EQUAL(0, 
                ham_set_compare_func(db, &my_compare_func_u32));
        
        ham_key_t key;
        ham_record_t rec;
        const int vals[] =
        { 1, 7, 3, 2, 9, 55, 42, 660, 14, 11, 37, 99, 
          123, 111, 459, 52, 66, 77, 88, 915, 31415, 12719 };

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        my_key_t my_key = {666};
        key.data = &my_key;
        key.size = MY_KEY_SIZE;
        key.flags = HAM_KEY_USER_ALLOC;
        
        /* empty DB: LT/GT must turn up error */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db, 0, &key, &rec, HAM_FIND_EXACT_MATCH));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db, 0, &key, &rec, HAM_FIND_LEQ_MATCH));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db, 0, &key, &rec, HAM_FIND_LT_MATCH));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db, 0, &key, &rec, HAM_FIND_GT_MATCH));

        int fill = 0;
        my_rec_t my_rec = {1000, "hello world!"};
        rec.data = &my_rec;
        rec.size = sizeof(my_rec);
        rec.flags = HAM_RECORD_USER_ALLOC;

        my_key.key_val1 = vals[fill++];

        BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        
        /* one record in DB: LT/GT must turn up that one for the 
         * right key values */
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, HAM_FIND_EXACT_MATCH));
        BFC_ASSERT_NOTEQUAL((rec.data && key.data), 0);
        my_rec_t *r = (my_rec_t *)rec.data;
        my_key_t *k = (my_key_t *)key.data;
        BFC_ASSERT_EQUAL(r->rec_val1, 1000);
        BFC_ASSERT_EQUAL(k->key_val1, (ham_u32_t)vals[fill-1]);
        
        ::memset(&rec, 0, sizeof(rec));
        key.data = &my_key;
        key.size = MY_KEY_SIZE;
        key.flags = HAM_KEY_USER_ALLOC;
        BFC_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, HAM_FIND_NEAR_MATCH));
        BFC_ASSERT_NOTEQUAL((rec.data && key.data), 0);
        r = (my_rec_t *)rec.data;
        k = (my_key_t *)key.data;
        BFC_ASSERT_EQUAL(r->rec_val1, 1000);
        BFC_ASSERT_EQUAL(k->key_val1, (ham_u32_t)vals[fill-1]);
        BFC_ASSERT_EQUAL(ham_key_get_approximate_match_type(&key), 0);
        
        ::memset(&rec, 0, sizeof(rec));
        my_key.key_val1 = vals[fill-1] - 1;
        key.data = &my_key;
        key.size = MY_KEY_SIZE;
        key.flags = HAM_KEY_USER_ALLOC;
        BFC_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, HAM_FIND_NEAR_MATCH));
        BFC_ASSERT_NOTEQUAL((rec.data && key.data), 0);
        r = (my_rec_t *)rec.data;
        k = (my_key_t *)key.data;
        BFC_ASSERT_EQUAL(r->rec_val1, 1000);
        BFC_ASSERT_EQUAL(k->key_val1, (ham_u32_t)vals[fill-1]);
        BFC_ASSERT_EQUAL(ham_key_get_approximate_match_type(&key), 1);
        
        ::memset(&rec, 0, sizeof(rec));
        my_key.key_val1 = vals[fill-1] + 2;
        key.data = &my_key;
        key.size = MY_KEY_SIZE;
        key.flags = HAM_KEY_USER_ALLOC;
        BFC_ASSERT_EQUAL(0, ham_find(db, 0, &key, &rec, HAM_FIND_NEAR_MATCH));
        BFC_ASSERT_NOTEQUAL((rec.data && key.data), 0);
        r = (my_rec_t *)rec.data;
        k = (my_key_t *)key.data;
        BFC_ASSERT_EQUAL(r->rec_val1, 1000);
        BFC_ASSERT_EQUAL(k->key_val1, (ham_u32_t)vals[fill-1]);
        BFC_ASSERT_EQUAL(ham_key_get_approximate_match_type(&key), -1);

        key.data = (void *)&my_key;
        key.size = MY_KEY_SIZE;
        key.flags = HAM_KEY_USER_ALLOC;
        
        /* add two more records */
        unsigned int i;
        for (i = 0; i < 2; i++)
        {
            my_rec.rec_val1 = 2000 + i;
            rec.data = &my_rec;
            rec.size = sizeof(my_rec);
            rec.flags = HAM_RECORD_USER_ALLOC;

            my_key.key_val1 = vals[fill++];
            key.data = (void *)&my_key;
            key.size = MY_KEY_SIZE;
            key.flags = HAM_KEY_USER_ALLOC;

            BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        }

        /* show record collection */
        const int verify_vals1[] =
        { 1, 3, 7 };
        ham_cursor_t *cursor;
        BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));
        for (i = 0; i < 3; i++)
        {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
            BFC_ASSERT_NOTEQUAL((rec.data && key.data), 0);
            r = (my_rec_t *)rec.data;
            k = (my_key_t *)key.data;
            BFC_ASSERT_NOTEQUAL(r->rec_val1, 0);
            BFC_ASSERT_EQUAL(k->key_val1, (ham_u32_t)verify_vals1[i]);
        }
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));


        /* three records in DB {1, 3, 7}: LT/GT should pick the 
         * 'proper' one each time */
        struct search_res_t
        {
            int rv;
            int keyval;
            int sign;
            int recval;
        };
        struct search_cat_t
        {
            ham_u32_t mode;
            const struct search_res_t *cases;
            const char *descr;
        };
        int srch_vals1[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8 };
        const search_res_t srch_res_any1[] = { 
            { 0, 1, 1, 1000}, 
            { 0, 1, 0, 1000}, 
            { 0, 1, -1, 1000}, /* {2, ...} would've been OK too, but 
                                  we just happen to know the 'near' 
                                  internals... */
            { 0, 3, 0, 2001},
            { 0, 3, -1, 2001}, /* be reminded: this is NOT really 'nearest' 
                                  search, just a kind of 'next-door neighbour 
                                  search' ... with favorite neighbours ;-) */
            { 0, 3, -1, 2001},
            { 0, 3, -1, 2001},
            { 0, 7, 0, 2000},
            { 0, 7, -1, 2000}
        };
        const search_res_t srch_res_leq1[] = { 
            { HAM_KEY_NOT_FOUND, 0, 0, 666}, 
            { 0, 1, 0, 1000}, 
            { 0, 1, -1, 1000},
            { 0, 3, 0, 2001},
            { 0, 3, -1, 2001},
            { 0, 3, -1, 2001},
            { 0, 3, -1, 2001},
            { 0, 7, 0, 2000},
            { 0, 7, -1, 2000}
        };
        const search_res_t srch_res_lt1[] = { 
            { HAM_KEY_NOT_FOUND, 0, 0, 666}, 
            { HAM_KEY_NOT_FOUND, 1, 0, 666}, 
            { 0, 1, -1, 1000},
            { 0, 1, -1, 1000},
            { 0, 3, -1, 2001},
            { 0, 3, -1, 2001},
            { 0, 3, -1, 2001},
            { 0, 3, -1, 2001},
            { 0, 7, -1, 2000}
        };
        const search_res_t srch_res_geq1[] = { 
            { 0, 1, 1, 1000}, 
            { 0, 1, 0, 1000}, 
            { 0, 3, 1, 2001},
            { 0, 3, 0, 2001},
            { 0, 7, 1, 2000},
            { 0, 7, 1, 2000},
            { 0, 7, 1, 2000},
            { 0, 7, 0, 2000},
            { HAM_KEY_NOT_FOUND, 8, 0, 666}
        };
        const search_res_t srch_res_gt1[] = { 
            { 0, 1, 1, 1000}, 
            { 0, 3, 1, 2001}, 
            { 0, 3, 1, 2001},
            { 0, 7, 1, 2000},
            { 0, 7, 1, 2000},
            { 0, 7, 1, 2000},
            { 0, 7, 1, 2000},
            { HAM_KEY_NOT_FOUND, 7, 0, 666},
            { HAM_KEY_NOT_FOUND, 8, 0, 666}
        };
        const search_res_t srch_res_eq1[] = { 
            { HAM_KEY_NOT_FOUND, 0, 0, 666}, 
            { 0, 1, 0, 1000}, 
            { HAM_KEY_NOT_FOUND, 2, 0, 666},
            { 0, 3, 0, 2001},
            { HAM_KEY_NOT_FOUND, 4, 0, 666},
            { HAM_KEY_NOT_FOUND, 5, 0, 666},
            { HAM_KEY_NOT_FOUND, 6, 0, 666},
            { 0, 7, 0, 2000},
            { HAM_KEY_NOT_FOUND, 8, 0, 666}
        };
        const search_cat_t srch_cats[] = {
            { HAM_FIND_NEAR_MATCH, srch_res_any1, "HAM_FIND_NEAR_MATCH '~'" },
            { HAM_FIND_LEQ_MATCH, srch_res_leq1, "HAM_FIND_LEQ_MATCH '<='" },
            { HAM_FIND_LT_MATCH, srch_res_lt1, "HAM_FIND_LT_MATCH '<'" },
            { HAM_FIND_GEQ_MATCH, srch_res_geq1, "HAM_FIND_GEQ_MATCH '>='" },
            { HAM_FIND_GT_MATCH, srch_res_gt1, "HAM_FIND_GT_MATCH '>'" },
            { HAM_FIND_EXACT_MATCH, srch_res_eq1, "HAM_FIND_EXACT_MATCH '='" },
            { 0 /* = HAM_FIND_EXACT_MATCH */, srch_res_eq1, 
                "zero default (0) '='" },
        };
        unsigned int j;

        for (j = 1; j < sizeof(srch_cats)/sizeof(srch_cats[0]); j++)
        {
            const search_res_t *res = srch_cats[j].cases;

            for (i = 0; i < sizeof(srch_vals1)/sizeof(srch_vals1[0]); i++)
            {
                // announce which test case is checked now; just reporting 
                // file+line+func isn't good enough here when things go pear 
                // shaped for a specific case...
                std::cout << "Test: category: " << srch_cats[j].descr 
                          << ", case: " << i << std::endl;

                ::memset(&key, 0, sizeof(key));
                ::memset(&rec, 0, sizeof(rec));
                my_key.key_val1 = srch_vals1[i];
                key.data = (void *)&my_key;
                key.size = MY_KEY_SIZE;
                key.flags = HAM_KEY_USER_ALLOC;
                int rv = ham_find(db, 0, &key, &rec, srch_cats[j].mode);
                //BFC_ASSERT_NOTEQUAL((rec.data && key.data), 0);
                r = (my_rec_t *)rec.data;
                k = (my_key_t *)key.data;
                BFC_ASSERT_EQUAL(rv, res[i].rv);
                BFC_ASSERT_EQUAL((r ? r->rec_val1 : 666), 
                        (ham_u32_t)res[i].recval);
                BFC_ASSERT_EQUAL((k ? k->key_val1 : 666), 
                        (ham_u32_t)res[i].keyval);
                BFC_ASSERT_EQUAL(ham_key_get_approximate_match_type(&key), 
                        res[i].sign);
            }
        }

        /* add more records: fill one page; then in the next 
         * round overflow by one, and then on to three pages, etc. */
        for (i = 0; i < keycount - 3+1; i++)
        {
            my_rec.rec_val1 = 3000 + i;
            rec.data = &my_rec;
            rec.size = sizeof(my_rec);
            rec.flags = HAM_RECORD_USER_ALLOC;

            my_key.key_val1 = vals[fill++];
            key.data = (void *)&my_key;
            key.size = MY_KEY_SIZE;
            key.flags = HAM_KEY_USER_ALLOC;

            BFC_ASSERT_EQUAL_I(0, ham_insert(db, 0, &key, &rec, 0), i);
        }

        /* show record collection */
        const int verify_vals2[] =
        { 1, 2, 3, 7, 9, 14, 42, 55, 660 };
        BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));
        for (i = 0; i < 9; i++)
        {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
            BFC_ASSERT_NOTEQUAL((rec.data && key.data), 0);
            r = (my_rec_t *)rec.data;
            k = (my_key_t *)key.data;
            BFC_ASSERT_NOTEQUAL(r->rec_val1, 0);
            BFC_ASSERT_EQUAL(k->key_val1, (ham_u32_t)verify_vals2[i]);
        }
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        
        BFC_ASSERT_EQUAL(0, ham_close(db, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
    }

    void insertTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(0, 0, &key, &rec, 0));
        key.flags=0x13;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, 0));
        key.flags=0;
        rec.flags=0x13;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, 0));
        rec.flags=0;
        key.flags=HAM_KEY_USER_ALLOC;
        BFC_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        key.flags=0;
        rec.flags=HAM_RECORD_USER_ALLOC;
        BFC_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        rec.flags=0;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE|HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, 0, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, 0, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE_INSERT_BEFORE));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE_INSERT_AFTER));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE_INSERT_FIRST));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE_INSERT_LAST));
        BFC_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
    }

    void insertDuplicateTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE|HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ham_db_t *db;
        BFC_ASSERT_EQUAL(0, ham_new(&db));

        BFC_ASSERT_EQUAL(0, 
                ham_create(db, BFC_OPATH(".test"), HAM_ENABLE_DUPLICATES, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE|HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
    }

    void insertBigKeyTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        char buffer[0xffff];
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        ::memset(buffer, 0, sizeof(buffer));
        key.size=sizeof(buffer);
        key.data=buffer;

        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
    }

    void eraseTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_erase(0, 0, &key, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_erase(m_db, 0, 0, 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_erase(m_db, 0, &key, 0));
    }

    void flushTest(void)
    {
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_flush(0, 0));
    }

    void flushBackendTest(void)
    {
        ham_env_t *env1, *env2;
        ham_db_t *db1, *db2;

        ham_key_t key;
        ham_record_t rec;
        int value=1;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        key.data=&value;
        key.size=sizeof(value);

        BFC_ASSERT_EQUAL(0, ham_env_new(&env1));
        BFC_ASSERT_EQUAL(0, ham_new(&db1));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env1, BFC_OPATH(".test"), 
                    HAM_LOCK_EXCLUSIVE, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env1, db1, 111, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db1, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_flush(db1, 0));

        BFC_ASSERT_EQUAL(0, ham_env_new(&env2));
        BFC_ASSERT_EQUAL(0, ham_new(&db2));
        /* Exclusive locking is now the default */
        BFC_ASSERT_EQUAL(HAM_WOULD_BLOCK, 
                ham_env_open(env2, BFC_OPATH(".test"), HAM_LOCK_EXCLUSIVE));
        BFC_ASSERT_EQUAL(0, ham_env_close(env2, 0));
        BFC_ASSERT_EQUAL(HAM_WOULD_BLOCK, 
                ham_env_open(env2, BFC_OPATH(".test"), 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env1, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env2, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env2, BFC_OPATH(".test"), HAM_READ_ONLY));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env2, db2, 111, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db2, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db2, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env2, 0));

        BFC_ASSERT_EQUAL(0, ham_close(db1, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(db1));
        BFC_ASSERT_EQUAL(0, ham_env_close(env1, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(db2));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env1));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env2));
    }

    void closeTest(void)
    {
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_close(0, 0));

        ham_db_t db;
        memset(&db, 0, sizeof(db));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_close(&db, HAM_TXN_AUTO_ABORT|HAM_TXN_AUTO_COMMIT));
    }

    void closeWithCursorsTest(void)
    {
        ham_cursor_t *c[5];

        for (int i=0; i<5; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c[i]));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        for (int i=0; i<5; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_close(c[i]));
    }

    void closeWithCursorsAutoCleanupTest(void)
    {
        ham_cursor_t *c[5];

        for (int i=0; i<5; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c[i]));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_AUTO_CLEANUP));
    }

    void compareTest(void)
    {
        ham_compare_func_t f=my_compare_func;

        BFC_ASSERT_EQUAL(0, ham_set_compare_func(m_db, f));
        BFC_ASSERT_EQUAL(f, db_get_compare_func(m_db));

        f=db_default_compare;
        BFC_ASSERT_EQUAL(0, ham_set_compare_func(m_db, 0));
        BFC_ASSERT(f==db_get_compare_func(m_db));
    }

    void prefixCompareTest(void)
    {
        ham_prefix_compare_func_t f=my_prefix_compare_func;

        BFC_ASSERT_EQUAL(0, 
                ham_set_prefix_compare_func(m_db, f));
        BFC_ASSERT_EQUAL(f, db_get_prefix_compare_func(m_db));

        BFC_ASSERT_EQUAL(0, ham_set_prefix_compare_func(m_db, 0));
        BFC_ASSERT(0==db_get_prefix_compare_func(m_db));
    }

    void cursorCreateTest(void)
    {
        ham_cursor_t *cursor;

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_create(0, 0, 0, &cursor));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_create(m_db, 0, 0, 0));
    }

    void cursorCloneTest(void)
    {
        ham_cursor_t src, *dest;

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_clone(0, &dest));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_clone(&src, 0));
    }

    void cursorMoveTest(void)
    {
        ham_cursor_t *cursor;
        ham_key_t key;
        ::memset(&key, 0, sizeof(key));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_move(0, 0, 0, 0));
        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                ham_cursor_move(cursor, &key, 0, 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, 0, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, 0, HAM_CURSOR_PREVIOUS));

        ham_cursor_close(cursor);
    }

    void cursorReplaceTest(void)
    {
        ham_cursor_t *cursor;
        ham_record_t *record=0;

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_overwrite(0, record, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_overwrite(cursor, 0, 0));

        ham_cursor_close(cursor);
    }

    void cursorFindTest(void)
    {
        ham_cursor_t *cursor;
        ham_key_t *key=0;

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_find(0, key, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_find(cursor, 0, 0));

        ham_cursor_close(cursor);
    }

    void cursorInsertTest(void)
    {
        ham_cursor_t *cursor;
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(cursor, 0, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(cursor, &key, 0, 0));

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    }

    void cursorEraseTest(void)
    {
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_erase(0, 0));
    }

    void cursorCloseTest(void)
    {
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_close(0));
    }

    void cursorGetErasedItemTest(void)
    {
        ham_db_t *db;
        ham_cursor_t *cursor;
        ham_key_t key;
        ham_record_t rec;
        int value=0;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        key.data=&value;
        key.size=sizeof(value);

        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, ham_create(db, BFC_OPATH(".test"), 0, 0664));

        value=1;
        BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        value=2;
        BFC_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));
        value=1;
        BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, ham_erase(db, 0, &key, 0));
        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                ham_cursor_move(cursor, &key, 0, 0));

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
    }

    void replaceKeyTest(void)
    {
        /* in-memory */
        ham_key_t key;
        ham_record_t rec;
        char buffer1[32], buffer2[7];
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        ::memset(buffer1, 0, sizeof(buffer1));
        ::memset(buffer2, 0, sizeof(buffer2));
        rec.size=sizeof(buffer1);
        rec.data=buffer1;

        /* insert a big blob */
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_size_t)sizeof(buffer1), rec.size);
        BFC_ASSERT_EQUAL(0, ::memcmp(rec.data, buffer1, sizeof(buffer1)));

        /* replace with a tiny blob */
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.size=sizeof(buffer2);
        rec.data=buffer2;
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_size_t)sizeof(buffer2), rec.size);
        BFC_ASSERT_EQUAL(0, ::memcmp(rec.data, buffer2, sizeof(buffer2)));

        /* replace with a big blob */
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.size=sizeof(buffer1);
        rec.data=buffer1;
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_size_t)sizeof(buffer1), rec.size);
        BFC_ASSERT_EQUAL(0, ::memcmp(rec.data, buffer1, sizeof(buffer1)));

        /* replace with a NULL blob */
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.size=0;
        rec.data=0;
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, rec.size);
        BFC_ASSERT(rec.data==0);

        /* replace with a tiny blob */
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.size=sizeof(buffer2);
        rec.data=buffer2;
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_size_t)sizeof(buffer2), rec.size);
        BFC_ASSERT_EQUAL(0, ::memcmp(rec.data, buffer2, sizeof(buffer2)));

        /* replace with a NULL blob */
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.size=0;
        rec.data=0;
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, rec.size);
        BFC_ASSERT(rec.data==0);
    }

    void replaceKeyFileTest(void)
    {
        ham_db_t *olddb=m_db;
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, ham_create(m_db, BFC_OPATH(".test"), 0, 0664));
        replaceKeyTest();
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(m_db));
        m_db=olddb;
    }

    void callocTest() {
        char *p=(char *)ham_mem_calloc(m_db, 20);

        for (int i=0; i<20; i++) {
            BFC_ASSERT_EQUAL('\0', p[i]);
        }

        ham_mem_free(m_db, p);
    }

    void strerrorTest() {
        for (int i=-300; i<=0; i++) {
            BFC_ASSERT(ham_strerror((ham_status_t)i)!=0);
        }
        BFC_ASSERT_EQUAL(0, strcmp("Unknown error", 
                    ham_strerror((ham_status_t)-204)));
        BFC_ASSERT_EQUAL(0, strcmp("Unknown error", 
                    ham_strerror((ham_status_t)-35)));
        BFC_ASSERT_EQUAL(0, strcmp("Unknown error", 
                    ham_strerror((ham_status_t)1)));
    }

    void contextDataTest() {
        void *ptr=(void *)0x13;
        ham_set_context_data(0, 0);
        ham_set_context_data(m_db, ptr);
        BFC_ASSERT_EQUAL((void *)0, ham_get_context_data(0));
        BFC_ASSERT_EQUAL((void *)0x13, ham_get_context_data(m_db));
        ham_set_context_data(m_db, 0);
        BFC_ASSERT_EQUAL((void *)0, ham_get_context_data(m_db));
    }

    void recoveryTest() {
        ham_db_t *olddb=m_db;
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, ham_create(m_db, BFC_OPATH(".test"), 
                                HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(m_db));
        m_db=olddb;
    }

    void recoveryNegativeTest() {
        ham_db_t *olddb=m_db;
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_RECOVERY|HAM_IN_MEMORY_DB, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_RECOVERY|HAM_WRITE_THROUGH, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(m_db, BFC_OPATH(".test"), 
                        HAM_ENABLE_RECOVERY|HAM_DISABLE_FREELIST_FLUSH, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(m_db));
        m_db=olddb;
    }

    void recoveryEnvTest() {
        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, BFC_OPATH(".test"), HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void recoveryEnvNegativeTest() {
        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_create(env, BFC_OPATH(".test"), 
                        HAM_ENABLE_RECOVERY|HAM_IN_MEMORY_DB, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_create(env, BFC_OPATH(".test"), 
                        HAM_ENABLE_RECOVERY|HAM_WRITE_THROUGH, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_create(env, BFC_OPATH(".test"), 
                        HAM_ENABLE_RECOVERY|HAM_DISABLE_FREELIST_FLUSH, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void btreeMacroTest(void)
    {
        ham_page_t *page=db_alloc_page(m_db, 0, 0);
        BFC_ASSERT(page!=0);

        int off=(int)btree_node_get_key_offset(page, 0);
        int l = db_get_persistent_header_size(); // 12
        l += OFFSETOF(btree_node_t, _entries); // 40-12
     
        l = db_get_int_key_header_size();
        l += db_get_keysize(page_get_owner(page));

        BFC_ASSERT_EQUAL((int)page_get_self(page)+12+28, off);
        off=(int)btree_node_get_key_offset(page, 1);
        BFC_ASSERT_EQUAL((int)page_get_self(page)+12+28+32, off);
        off=(int)btree_node_get_key_offset(page, 2);
        BFC_ASSERT_EQUAL((int)page_get_self(page)+12+28+64, off);

        db_free_page(page, 0);
    }

    void cursorInsertAppendTest(void)
    {
        ham_cursor_t *cursor;
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));
        for (unsigned i=0; i<10000; i++) {
            key.size=sizeof(i);
            key.data=(void *)&i;
            rec.size=sizeof(i);
            rec.data=(void *)&i;
            BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 
                HAM_HINT_SEQUENTIAL));
        }
        for (unsigned i=0; i<10000; i++) {
            key.size=sizeof(i);
            key.data=(void *)&i;
            BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL(key.size, rec.size);
            BFC_ASSERT_EQUAL(0, memcmp(key.data, rec.data, key.size));
        }
        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    }

    void negativeCursorInsertAppendTest(void)
    {
        ham_cursor_t *cursor;
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));
        for (unsigned i=10; i>0; i--) {
            key.size=sizeof(i);
            key.data=(void *)&i;
            rec.size=sizeof(i);
            rec.data=(void *)&i;
            BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 
                HAM_HINT_SEQUENTIAL));
        }
        for (unsigned i=1; i<=10; i++) {
            key.size=sizeof(i);
            key.data=(void *)&i;
            BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL(key.size, rec.size);
            BFC_ASSERT_EQUAL(0, memcmp(key.data, rec.data, key.size));
        }
        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    }

    void recordCountTest(void)
    {
        ham_cursor_t *cursor;
        ham_key_t key;
        ham_record_t rec;
        ham_offset_t count;

        ham_parameter_t ps[]={{HAM_PARAM_PAGESIZE,   1024*4}, {0, 0}};

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(m_db));
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));

        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, BFC_OPATH(".test"), HAM_ENABLE_DUPLICATES, 0664, ps));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));
        for (unsigned i=4000; i>0; i--) 
        {
            key.size=sizeof(i);
            key.data=(void *)&i;
            rec.size=sizeof(i);
            rec.data=(void *)&i;
            BFC_ASSERT_EQUAL_I(0, ham_cursor_insert(cursor, &key, &rec, 
                HAM_HINT_SEQUENTIAL), i);
        }

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));

        for (unsigned i=1; i<=10; i++) {
            unsigned k = 5;
            key.size=sizeof(k);
            key.data=(void *)&k;
            rec.size=sizeof(i);
            rec.data=(void *)&i;
            BFC_ASSERT_EQUAL_I(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE), i);
        }

        count = 0;
        BFC_ASSERT_EQUAL(0, 
                ham_get_key_count(m_db, 0, HAM_HINT_UBER_FAST_ACCESS, &count));
        BFC_ASSERT_EQUAL(4000, count);

        BFC_ASSERT_EQUAL(0, 
                ham_get_key_count(m_db, 0, HAM_SKIP_DUPLICATES, &count));
        BFC_ASSERT_EQUAL(4000, count);

        BFC_ASSERT_EQUAL(0, 
                ham_get_key_count(m_db, 0, 0, &count));
        BFC_ASSERT_EQUAL(4000+10, count);
    }

};

BFC_REGISTER_FIXTURE(HamsterdbTest);

