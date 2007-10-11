/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 */

#include <stdexcept>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "memtracker.h"
#include "../src/db.h"
#include "../src/version.h"

static int
my_compare_func(const ham_u8_t *lhs, ham_size_t lhs_length,
                const ham_u8_t *rhs, ham_size_t rhs_length)
{
    (void)lhs;
    (void)rhs;
    (void)lhs_length;
    (void)rhs_length;
    return (0);
}

static int
my_prefix_compare_func(const ham_u8_t *lhs, ham_size_t lhs_length,
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

class HamsterdbTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(HamsterdbTest);
    CPPUNIT_TEST      (versionTest);
    CPPUNIT_TEST      (newTest);
    CPPUNIT_TEST      (deleteTest);
    CPPUNIT_TEST      (openTest);
    CPPUNIT_TEST      (createTest);
    CPPUNIT_TEST      (createCloseCreateTest);
    CPPUNIT_TEST      (createPagesizeReopenTest);
    CPPUNIT_TEST      (readOnlyTest);
    CPPUNIT_TEST      (invalidPagesizeTest);
    CPPUNIT_TEST      (getErrorTest);
    CPPUNIT_TEST      (setPrefixCompareTest);
    CPPUNIT_TEST      (setCompareTest);
    CPPUNIT_TEST      (findTest);
    CPPUNIT_TEST      (insertTest);
    CPPUNIT_TEST      (insertBigKeyTest);
    CPPUNIT_TEST      (eraseTest);
    CPPUNIT_TEST      (flushTest);
    CPPUNIT_TEST      (flushBackendTest);
    CPPUNIT_TEST      (closeTest);
    CPPUNIT_TEST      (closeWithCursorsTest);
    CPPUNIT_TEST      (closeWithCursorsOkTest);
    CPPUNIT_TEST      (compareTest);
    CPPUNIT_TEST      (prefixCompareTest);
    CPPUNIT_TEST      (cursorCreateTest);
    CPPUNIT_TEST      (cursorCloneTest);
    CPPUNIT_TEST      (cursorMoveTest);
    CPPUNIT_TEST      (cursorReplaceTest);
    CPPUNIT_TEST      (cursorFindTest);
    CPPUNIT_TEST      (cursorInsertTest);
    CPPUNIT_TEST      (cursorEraseTest);
    CPPUNIT_TEST      (cursorCloseTest);
    CPPUNIT_TEST      (cursorGetErasedItemTest);
    CPPUNIT_TEST      (replaceKeyTest);
    CPPUNIT_TEST      (replaceKeyFileTest);
    CPPUNIT_TEST      (callocTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
#if WIN32
        (void)DeleteFileA((LPCSTR)".test");
#else
        (void)unlink(".test");
#endif
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, 0, HAM_IN_MEMORY_DB, 0));
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void versionTest(void)
    {
        ham_u32_t major, minor, revision;

        ham_get_version(&major, &minor, &revision);

        CPPUNIT_ASSERT_EQUAL((ham_u32_t)HAM_VERSION_MAJ, major);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)HAM_VERSION_MIN, minor);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)HAM_VERSION_REV, revision);
    };

    void newTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, ham_new(0));
    }

    void deleteTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, ham_delete(0));
    }

    void openTest(void)
    {
        ham_db_t *db;

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open(0, "test.db", 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open(db, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_open(db, 0, HAM_IN_MEMORY_DB));
        CPPUNIT_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, 
                ham_open(db, "xxxx...", 0));

#if WIN32
        CPPUNIT_ASSERT_EQUAL(HAM_IO_ERROR, 
                ham_open(db, "c:\\windows", 0));
#else
        CPPUNIT_ASSERT_EQUAL(HAM_IO_ERROR, 
                ham_open(db, "/usr", 0));
#endif

        ham_delete(db);
    }

    void createTest(void)
    {
        ham_db_t *db;
        ham_parameter_t cs[]={{HAM_PARAM_CACHESIZE, 1024}, {0, 0}};
        ham_parameter_t ps[]={{HAM_PARAM_PAGESIZE,   512}, {0, 0}};

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(0, "test.db", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(db, 0, 0, 0664));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(db, 0, HAM_IN_MEMORY_DB|HAM_CACHE_STRICT, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create_ex(db, 0, HAM_IN_MEMORY_DB, 0, &cs[0]));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create_ex(db, 0, HAM_IN_MEMORY_DB|HAM_READ_ONLY, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create_ex(db, 0, HAM_READ_ONLY, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PAGESIZE, 
                ham_create_ex(db, ".test", 0, 0, &ps[0]));
#if WIN32
        CPPUNIT_ASSERT_EQUAL(HAM_IO_ERROR, 
                ham_create(db, "c:\\windows", 0, 0664));
#else
        CPPUNIT_ASSERT_EQUAL(HAM_IO_ERROR, 
                ham_create(db, "/home", 0, 0664));
#endif
        ham_delete(db);
    }

    void createCloseCreateTest(void)
    {
        ham_db_t *db;

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));

        CPPUNIT_ASSERT_EQUAL(0, ham_create(db, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_open(db, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));

        ham_delete(db);
    }

    void createPagesizeReopenTest(void)
    {
        ham_db_t *db;
        ham_parameter_t ps[]={{HAM_PARAM_PAGESIZE,   1024*128}, {0, 0}};

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));

        CPPUNIT_ASSERT_EQUAL(0, ham_create_ex(db, ".test", 0, 0664, &ps[0]));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_open(db, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
    }

    void readOnlyTest(void)
    {
        ham_db_t *db;
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *cursor;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));

        CPPUNIT_ASSERT_EQUAL(0, ham_create(db, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_open(db, ".test", HAM_READ_ONLY));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));

        CPPUNIT_ASSERT_EQUAL(HAM_DB_READ_ONLY, 
                ham_insert(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_DB_READ_ONLY, 
                ham_erase(db, 0, &key, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_DB_READ_ONLY, 
                ham_cursor_overwrite(cursor, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_DB_READ_ONLY, 
                ham_cursor_insert(cursor, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_DB_READ_ONLY, 
                ham_cursor_erase(cursor, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
    }

    void invalidPagesizeTest(void)
    {
        ham_db_t *db;
        ham_parameter_t p[]={
            {HAM_PARAM_PAGESIZE, 1024}, 
            {HAM_PARAM_KEYSIZE,   512}, 
            {0, 0}
        };

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_KEYSIZE, 
                ham_create_ex(db, ".test", 0, 0664, &p[0]));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
    }

    void getErrorTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(0, ham_get_error(0));
    }

    void setPrefixCompareTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_set_prefix_compare_func(0, 0));
    }

    void setCompareTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_set_compare_func(0, 0));
    }

    void findTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_find(0, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_find(m_db, 0, 0, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_find(m_db, 0, &key, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(m_db, 0, &key, &rec, 0));
    }

    void insertTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(0, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, 0, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE_INSERT_BEFORE));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE_INSERT_AFTER));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE_INSERT_FIRST));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE_INSERT_LAST));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, 0));
    }

    void insertDuplicateTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE|HAM_OVERWRITE));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ham_db_t *db;
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(db, ".test", HAM_ENABLE_DUPLICATES, 0664));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE|HAM_OVERWRITE));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
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

        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
    }

    void eraseTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_erase(0, 0, &key, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_erase(m_db, 0, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_erase(m_db, 0, &key, 0));
    }

    void flushTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
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

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env1));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db1));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env1, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env1, db1, 111, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db1, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_flush(db1, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env2));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db2));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env2, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env2, db2, 111, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db2, 0, &key, &rec, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_close(db1, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db1));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env1, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env1));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db2, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db2));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env2, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env2));
    }

    void closeTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, ham_close(0, 0));
    }

    void closeWithCursorsTest(void)
    {
        ham_cursor_t *c;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        CPPUNIT_ASSERT_EQUAL(HAM_DB_NOT_EMPTY, ham_close(m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void closeWithCursorsOkTest(void)
    {
        ham_cursor_t *c[5];

        for (int i=0; i<5; i++)
            CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c[i]));

        CPPUNIT_ASSERT_EQUAL(HAM_DB_NOT_EMPTY, ham_close(m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_AUTO_CLEANUP));
    }

    void compareTest(void)
    {
        ham_compare_func_t f=my_compare_func;

        CPPUNIT_ASSERT_EQUAL(0, ham_set_compare_func(m_db, f));
        CPPUNIT_ASSERT_EQUAL(f, db_get_compare_func(m_db));

        f=db_default_compare;
        CPPUNIT_ASSERT_EQUAL(0, ham_set_compare_func(m_db, 0));
        CPPUNIT_ASSERT(f==db_get_compare_func(m_db));
    }

    void prefixCompareTest(void)
    {
        ham_prefix_compare_func_t f=my_prefix_compare_func;

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_set_prefix_compare_func(m_db, f));
        CPPUNIT_ASSERT_EQUAL(f, db_get_prefix_compare_func(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_set_prefix_compare_func(m_db, 0));
        f=db_default_prefix_compare;
        CPPUNIT_ASSERT_EQUAL(f, db_get_prefix_compare_func(m_db));
    }

    void cursorCreateTest(void)
    {
        ham_cursor_t *cursor;

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_create(0, 0, 0, &cursor));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_create(m_db, 0, 0, 0));
    }

    void cursorCloneTest(void)
    {
        ham_cursor_t src, *dest;

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_clone(0, &dest));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_clone(&src, 0));
    }

    void cursorMoveTest(void)
    {
        ham_cursor_t *cursor;
        ham_key_t key;
        ::memset(&key, 0, sizeof(key));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_move(0, 0, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                ham_cursor_move(cursor, &key, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, 0, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, 0, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, 0, HAM_CURSOR_NEXT));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, 0, HAM_CURSOR_PREVIOUS));

        ham_cursor_close(cursor);
    }

    void cursorReplaceTest(void)
    {
        ham_cursor_t *cursor;
        ham_record_t *record=0;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_overwrite(0, record, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_overwrite(cursor, 0, 0));

        ham_cursor_close(cursor);
    }

    void cursorFindTest(void)
    {
        ham_cursor_t *cursor;
        ham_key_t *key=0;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_find(0, key, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
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

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(cursor, 0, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(cursor, &key, 0, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    }

    void cursorEraseTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_erase(0, 0));
    }

    void cursorCloseTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
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

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, ham_create(db, ".test", 0, 0664));

        value=1;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));
        value=2;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db, 0, &key, &rec, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));
        value=1;
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_erase(db, 0, &key, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                ham_cursor_move(cursor, &key, 0, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
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
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(buffer1), rec.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(rec.data, buffer1, sizeof(buffer1)));

        /* replace with a tiny blob */
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.size=sizeof(buffer2);
        rec.data=buffer2;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(buffer2), rec.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(rec.data, buffer2, sizeof(buffer2)));

        /* replace with a big blob */
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.size=sizeof(buffer1);
        rec.data=buffer1;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(buffer1), rec.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(rec.data, buffer1, sizeof(buffer1)));

        /* replace with a NULL blob */
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.size=0;
        rec.data=0;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, rec.size);
        CPPUNIT_ASSERT(rec.data==0);

        /* replace with a tiny blob */
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.size=sizeof(buffer2);
        rec.data=buffer2;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(buffer2), rec.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(rec.data, buffer2, sizeof(buffer2)));

        /* replace with a NULL blob */
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.size=0;
        rec.data=0;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, rec.size);
        CPPUNIT_ASSERT(rec.data==0);
    }

    void replaceKeyFileTest(void)
    {
        ham_db_t *olddb=m_db;
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 0, 0664));
        replaceKeyTest();
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(m_db));
        m_db=olddb;
    }

    void callocTest() {
        char *p=(char *)ham_mem_calloc(m_db, 20);

        for (int i=0; i<20; i++) {
            CPPUNIT_ASSERT_EQUAL('\0', p[i]);
        }

        ham_mem_free(m_db, p);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(HamsterdbTest);

