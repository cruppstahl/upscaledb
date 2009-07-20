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
#include "memtracker.h"
#include "../src/db.h"
#include "../src/version.h"
#include "../src/serial.h"
#include "../src/btree.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"

using namespace bfc;

static int
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

static int
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

class HamsterdbTest : public fixture
{
public:
    HamsterdbTest()
    :   fixture("HamsterdbTest")
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
        BFC_REGISTER_TEST(HamsterdbTest, getErrorTest);
        BFC_REGISTER_TEST(HamsterdbTest, setPrefixCompareTest);
        BFC_REGISTER_TEST(HamsterdbTest, setCompareTest);
        BFC_REGISTER_TEST(HamsterdbTest, findTest);
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
    }

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    void setup()
    { 
        os::unlink(".test");
        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL(0, ham_create(m_db, 0, HAM_IN_MEMORY_DB, 0));
    }
    
    void teardown() 
    { 
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
                ham_open(db, "data/inv-file-header.hdb", 0));

        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        ham_delete(db);
    }

    void invVersionTest(void)
    {
        ham_db_t *db;

        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(HAM_INV_FILE_VERSION, 
                ham_open(db, "data/inv-file-version.hdb", 0));

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
                ham_create_ex(db, ".test", 0, 0, &ps[0]));
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
                ham_create_ex(db, ".test", 0, 0644, &ps[0]));

        ps[0].value=1024;
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(db, ".test", 0, 0644, &ps[0]));
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
                ham_create_ex(db, ".test", 0, 0644, &ps[0]));

        ham_delete(db);
    }

    void createCloseCreateTest(void)
    {
        ham_db_t *db;

        BFC_ASSERT_EQUAL(0, ham_new(&db));

        BFC_ASSERT_EQUAL(0, ham_create(db, ".test", 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(db, ".test", 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));

        ham_delete(db);
    }

    void createPagesizeReopenTest(void)
    {
        ham_db_t *db;
        ham_parameter_t ps[]={{HAM_PARAM_PAGESIZE,   1024*128}, {0, 0}};

        BFC_ASSERT_EQUAL(0, ham_new(&db));

        BFC_ASSERT_EQUAL(0, ham_create_ex(db, ".test", 0, 0664, &ps[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(db, ".test", 0));
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

        BFC_ASSERT_EQUAL(0, ham_create(db, ".test", 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(db, ".test", HAM_READ_ONLY));
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
                ham_create_ex(db, ".test", 0, 0664, &p[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
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


static int my_prefix_compare_func_u32(ham_db_t *db, 
                                  const ham_u8_t *lhs, ham_size_t lhs_length, 
                                  ham_size_t lhs_real_length,
                                  const ham_u8_t *rhs, ham_size_t rhs_length,
                                  ham_size_t rhs_real_length)
{
	ham_u32_t *l = (ham_u32_t *)lhs;
	ham_u32_t *r = (ham_u32_t *)rhs;
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


static int my_compare_func_u32(ham_db_t *db, 
                                  const ham_u8_t *lhs, ham_size_t lhs_length, 
                                  const ham_u8_t *rhs, ham_size_t rhs_length)
{
	ham_u32_t *l = (ham_u32_t *)lhs;
	ham_u32_t *r = (ham_u32_t *)rhs;
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
		const int RECORD_COUNT_PER_DB = 100000;
		ham_env_t *env;
		ham_db_t *db;
        ham_parameter_t ps[]={
			{HAM_PARAM_PAGESIZE,   64*1024}, /* UNIX == WIN now */
	        {HAM_PARAM_CACHESIZE,  /*32*16*/ 4*64*1024},
			{0, 0}
		};
		struct my_key_t
		{
			ham_u32_t val1;
			ham_u32_t val2;
			ham_u32_t val3;
			ham_u32_t val4;
		};
		struct my_rec_t
		{
			ham_u32_t val1;
			ham_u32_t val2[15];
		};

		ham_key_t key;
        ham_record_t rec;

		my_key_t my_key;
		my_rec_t my_rec;
		
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_env_create_ex(env, ".test", 0 & HAM_DISABLE_MMAP, 0644, ps));
        
        BFC_ASSERT_EQUAL(0, ham_new(&db));
		//ham_size_t keycount = 0;
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 1, 0, NULL));
		//BFC_ASSERT_EQUAL(0, ham_calc_maxkeys_per_page(db, &keycount, sizeof(my_key)));
		BFC_ASSERT_EQUAL(0, ham_set_prefix_compare_func(db, &my_prefix_compare_func_u32));
		BFC_ASSERT_EQUAL(0, ham_set_compare_func(db, &my_compare_func_u32));
        
		std::cerr << "1K steps: ";

		/* insert the records: key=2*i; rec=100*i */
		ham_cursor_t *cursor;
		BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));
		int i;
		for (i = 0; i < RECORD_COUNT_PER_DB; i++)
		{
			::memset(&key, 0, sizeof(key));
			::memset(&rec, 0, sizeof(rec));
			::memset(&my_key, 0, sizeof(my_key));
			::memset(&my_rec, 0, sizeof(my_rec));

			my_rec.val1 = 100 * i;
			rec.data = &my_rec;
			rec.size = sizeof(my_rec);
			rec.flags = HAM_RECORD_USER_ALLOC;

			my_key.val1 = 2 * i;
			key.data = (void *)&my_key;
			key.size = sizeof(my_key);
			key.flags = HAM_KEY_USER_ALLOC;

			BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));

			if (i % 1000 == 999) {
				//std::cerr << ".";
				BFC_ASSERT_EQUAL(0, ham_check_integrity(db, NULL));
			}
		}
		BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));

		//std::cerr << std::endl;

		BFC_ASSERT_EQUAL(0, ham_check_integrity(db, NULL));

		my_rec_t *r;
		my_key_t *k;

		/* show record collection */
		BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));
		for (i = 0; i < RECORD_COUNT_PER_DB; i++)
		{
			::memset(&key, 0, sizeof(key));
			::memset(&rec, 0, sizeof(rec));
			BFC_ASSERT_EQUAL(0, ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
			BFC_ASSERT_NOTEQUAL((rec.data && key.data), 0);
			r = (my_rec_t *)rec.data;
			k = (my_key_t *)key.data;
#if 0
			printf("rec: %d vs. %d, ", r->val1, 100*i);
			printf("key: %d vs. %d\n", k->val1, 2*i);
#else
			BFC_ASSERT_EQUAL(r->val1, (ham_u32_t)100*i);
			BFC_ASSERT_EQUAL(k->val1, (ham_u32_t)2*i);
#endif
		}
		BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
		BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));

        BFC_ASSERT_EQUAL(0, ham_close(db, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
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
                ham_create(db, ".test", HAM_ENABLE_DUPLICATES, 0664));
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
        BFC_ASSERT_EQUAL(0, ham_env_create(env1, ".test", 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env1, db1, 111, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db1, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_flush(db1, 0));

        BFC_ASSERT_EQUAL(0, ham_env_new(&env2));
        BFC_ASSERT_EQUAL(0, ham_new(&db2));
        BFC_ASSERT_EQUAL(0, ham_env_open(env2, ".test", 0));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env2, db2, 111, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db2, 0, &key, &rec, 0));

        BFC_ASSERT_EQUAL(0, ham_close(db1, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(db1));
        BFC_ASSERT_EQUAL(0, ham_env_close(env1, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env1));
        BFC_ASSERT_EQUAL(0, ham_close(db2, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(db2));
        BFC_ASSERT_EQUAL(0, ham_env_close(env2, 0));
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
        BFC_ASSERT_EQUAL(0, ham_create(db, ".test", 0, 0664));

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
        BFC_ASSERT_EQUAL(0, ham_create(m_db, ".test", 0, 0664));
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
        for (int i=-203; i<=0; i++) {
            BFC_ASSERT(ham_strerror((ham_status_t)i)!=0);
        }
        BFC_ASSERT_EQUAL(0, strcmp("Unknown error", 
                    ham_strerror((ham_status_t)-204)));
        BFC_ASSERT_EQUAL(0, strcmp("Unknown error", 
                    ham_strerror((ham_status_t)-30)));
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
        BFC_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                                HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(m_db));
        m_db=olddb;
    }

    void recoveryNegativeTest() {
        ham_db_t *olddb=m_db;
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(m_db, ".test", 
                        HAM_ENABLE_RECOVERY|HAM_IN_MEMORY_DB, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(m_db, ".test", 
                        HAM_ENABLE_RECOVERY|HAM_WRITE_THROUGH, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_create(m_db, ".test", 
                        HAM_ENABLE_RECOVERY|HAM_DISABLE_FREELIST_FLUSH, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(m_db));
        m_db=olddb;
    }

    void recoveryEnvTest() {
        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, ".test", HAM_ENABLE_RECOVERY, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void recoveryEnvNegativeTest() {
        ham_env_t *env;
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_create(env, ".test", 
                        HAM_ENABLE_RECOVERY|HAM_IN_MEMORY_DB, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_create(env, ".test", 
                        HAM_ENABLE_RECOVERY|HAM_WRITE_THROUGH, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_create(env, ".test", 
                        HAM_ENABLE_RECOVERY|HAM_DISABLE_FREELIST_FLUSH, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void btreeMacroTest(void)
    {
        ham_page_t *page=db_alloc_page(m_db, 0, 0);
        BFC_ASSERT(page!=0);

        int off=(int)btree_node_get_key_offset(page, 0);
        BFC_ASSERT_EQUAL((int)page_get_self(page)+11+28, off);
        off=(int)btree_node_get_key_offset(page, 1);
        BFC_ASSERT_EQUAL((int)page_get_self(page)+11+28+32, off);
        off=(int)btree_node_get_key_offset(page, 2);
        BFC_ASSERT_EQUAL((int)page_get_self(page)+11+28+64, off);

        db_free_page(page, 0);
    }

};

BFC_REGISTER_FIXTURE(HamsterdbTest);

