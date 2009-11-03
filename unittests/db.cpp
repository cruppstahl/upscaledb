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
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/cache.h"
#include "../src/page.h"
#include "../src/env.h"
#include "memtracker.h"
#include "../src/btree.h"
#include "../src/blob.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;


class DbTest : public hamsterDB_fixture
{
	define_super(hamsterDB_fixture);

public:
    DbTest(bool inmemory=false, const char *name="DbTest")
    :   hamsterDB_fixture(name),
        m_db(0), m_inmemory(inmemory), m_dev(0), m_alloc(0)
    {
        //if (name)
        //    return;
        testrunner::get_instance()->register_fixture(this);
		BFC_REGISTER_TEST(DbTest, checkStructurePackingTest);
        BFC_REGISTER_TEST(DbTest, headerTest);
        BFC_REGISTER_TEST(DbTest, structureTest);
        BFC_REGISTER_TEST(DbTest, envStructureTest);
        BFC_REGISTER_TEST(DbTest, defaultCompareTest);
        BFC_REGISTER_TEST(DbTest, defaultPrefixCompareTest);
        BFC_REGISTER_TEST(DbTest, allocPageTest);
        BFC_REGISTER_TEST(DbTest, fetchPageTest);
        BFC_REGISTER_TEST(DbTest, flushPageTest);
    }

protected:
    ham_db_t *m_db;
    ham_bool_t m_inmemory;
    ham_device_t *m_dev;
    memtracker_t *m_alloc;

public:
    virtual void setup() 
	{ 
		__super::setup();

        ham_page_t *p;
        BFC_ASSERT(0==ham_new(&m_db));
        m_alloc=memtracker_new(); //ham_default_allocator_new();
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT((m_dev=ham_device_new((mem_allocator_t *)m_alloc, 
                        db_get_env(m_db), m_inmemory))!=0);
        db_set_device(m_db, m_dev);
        BFC_ASSERT_EQUAL(0, m_dev->create(m_dev, BFC_OPATH(".test"), 0, 0644));
        p=page_new(m_db);
        BFC_ASSERT(0==page_alloc(p, device_get_pagesize(m_dev)));
        db_set_header_page(m_db, p);
        db_set_persistent_pagesize(m_db, m_dev->get_pagesize(m_dev));
        db_set_cooked_pagesize(m_db, device_get_pagesize(m_dev));
    }
    
    virtual void teardown() 
	{ 
		__super::teardown();

        if (db_get_header_page(m_db)) {
            page_free(db_get_header_page(m_db));
            page_delete(db_get_header_page(m_db));
            db_set_header_page(m_db, 0);
        }
        if (db_get_cache(m_db)) {
            cache_delete(m_db, db_get_cache(m_db));
            db_set_cache(m_db, 0);
        }
        if (db_get_device(m_db)) {
            if (db_get_device(m_db)->is_open(db_get_device(m_db)))
                db_get_device(m_db)->close(db_get_device(m_db));
            db_get_device(m_db)->destroy(db_get_device(m_db));
            db_set_device(m_db, 0);
        }
        ham_delete(m_db);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void headerTest()
    {
        db_set_magic(m_db, '1', '2', '3', '4');
        BFC_ASSERT(db_get_magic(db_get_header(m_db), 0)=='1');
        BFC_ASSERT(db_get_magic(db_get_header(m_db), 1)=='2');
        BFC_ASSERT(db_get_magic(db_get_header(m_db), 2)=='3');
        BFC_ASSERT(db_get_magic(db_get_header(m_db), 3)=='4');

        db_set_version(m_db, 1, 2, 3, 4);
        BFC_ASSERT(db_get_version(db_get_header(m_db), 0)==1);
        BFC_ASSERT(db_get_version(db_get_header(m_db), 1)==2);
        BFC_ASSERT(db_get_version(db_get_header(m_db), 2)==3);
        BFC_ASSERT(db_get_version(db_get_header(m_db), 3)==4);

        db_set_serialno(m_db, 0x1234);
        BFC_ASSERT(db_get_serialno(m_db)==0x1234);

        ham_size_t ps=db_get_cooked_pagesize(m_db); // since 1.1.0 cooked != persistent/raw necessarily
		ham_size_t raw_ps=db_get_persistent_pagesize(m_db);
        db_set_persistent_pagesize(m_db, 1024*32);
		BFC_ASSERT(db_get_cooked_pagesize(m_db)==ps);
        BFC_ASSERT(db_get_persistent_pagesize(m_db)==1024*32);
        db_set_cooked_pagesize(m_db, ps);
		db_set_persistent_pagesize(m_db, raw_ps);

        db_set_txn(m_db, (ham_txn_t *)13);
        BFC_ASSERT(db_get_txn(m_db)==(ham_txn_t *)13);

        db_set_extkey_cache(m_db, (extkey_cache_t *)14);
        BFC_ASSERT(db_get_extkey_cache(m_db)==(extkey_cache_t *)14);
    }

    void structureTest()
    {
        BFC_ASSERT(db_get_header_page(m_db)!=0);

        BFC_ASSERT(db_get_error(m_db)==HAM_SUCCESS);
        db_set_error(m_db, HAM_IO_ERROR);
        BFC_ASSERT(db_get_error(m_db)==HAM_IO_ERROR);

        BFC_ASSERT(db_get_backend(m_db)==0);
        db_set_backend(m_db, (ham_backend_t *)15);
        BFC_ASSERT(db_get_backend(m_db)==(ham_backend_t *)15);
        db_set_backend(m_db, 0);

        BFC_ASSERT(db_get_cache(m_db)==0);
        db_set_cache(m_db, (ham_cache_t *)16);
        BFC_ASSERT(db_get_cache(m_db)==(ham_cache_t *)16);
        db_set_cache(m_db, 0);

        BFC_ASSERT(db_get_prefix_compare_func(m_db)==0);
        db_set_prefix_compare_func(m_db, (ham_prefix_compare_func_t)18);
        BFC_ASSERT(db_get_prefix_compare_func(m_db)==
                    (ham_prefix_compare_func_t)18);

        BFC_ASSERT(db_get_compare_func(m_db)==0);
        db_set_compare_func(m_db, (ham_compare_func_t)19);
        BFC_ASSERT(db_get_compare_func(m_db)==(ham_compare_func_t)19);

        BFC_ASSERT(!db_is_dirty(m_db));
        db_set_dirty(m_db);
        BFC_ASSERT(db_is_dirty(m_db));

        BFC_ASSERT(db_get_rt_flags(m_db)==0);
        db_set_rt_flags(m_db, 20);
        BFC_ASSERT(db_get_rt_flags(m_db)==20);

        BFC_ASSERT(db_get_env(m_db)==0);
        db_set_env(m_db, (ham_env_t *)30);
        BFC_ASSERT(db_get_env(m_db)==(ham_env_t *)30);
        db_set_env(m_db, 0);

        BFC_ASSERT(db_get_next(m_db)==0);
        db_set_next(m_db, (ham_db_t *)40);
        BFC_ASSERT(db_get_next(m_db)==(ham_db_t *)40);

        BFC_ASSERT(db_get_record_allocsize(m_db)==0);
        db_set_record_allocsize(m_db, 21);
        BFC_ASSERT(db_get_record_allocsize(m_db)==21);
        db_set_record_allocsize(m_db, 0);

        BFC_ASSERT(db_get_record_allocdata(m_db)==0);
        db_set_record_allocdata(m_db, (void *)22);
        BFC_ASSERT(db_get_record_allocdata(m_db)==(void *)22);
        db_set_record_allocdata(m_db, 0);
    }

    void envStructureTest()
    {
        ham_env_t *env;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        env_set_txn_id(env, 0x12345ull);
        env_set_device(env, (ham_device_t *)0x13);
        env_set_cache(env, (ham_cache_t *)0x14);
        env_set_txn(env, (ham_txn_t *)0x16);
        env_set_extkey_cache(env, (extkey_cache_t *)0x17);
        env_set_rt_flags(env, 0x18);
        env_set_header_page(env, (ham_page_t *)0x19);
        env_set_list(env, m_db);

        db_set_env(m_db, env);

        BFC_ASSERT_EQUAL((ham_page_t *)0x19, 
                db_get_header_page(m_db));

        BFC_ASSERT_EQUAL((ham_cache_t *)0x14, 
                db_get_cache(m_db));

        BFC_ASSERT_EQUAL((ham_u32_t)0x18, db_get_rt_flags(m_db));

        BFC_ASSERT_EQUAL(env, db_get_env(m_db));

        env_set_device(env, 0);
        env_set_cache(env, 0);
        env_set_txn(env, 0);
        env_set_extkey_cache(env, 0);
        env_set_rt_flags(env, 0x18);
        env_set_header_page(env, 0);
        env_set_list(env, 0);
        db_set_env(m_db, 0);
        ham_env_delete(env);
    }

    void defaultCompareTest()
    {
        BFC_ASSERT( 0==db_default_compare(0,
                        (ham_u8_t *)"abc", 3, (ham_u8_t *)"abc", 3));
        BFC_ASSERT(-1==db_default_compare(0,
                        (ham_u8_t *)"ab",  2, (ham_u8_t *)"abc", 3));
        BFC_ASSERT(-1==db_default_compare(0,
                        (ham_u8_t *)"abc", 3, (ham_u8_t *)"bcd", 3));
        BFC_ASSERT(+1==db_default_compare(0,
                        (ham_u8_t *)"abc", 3, (ham_u8_t *)0,     0));
        BFC_ASSERT(-1==db_default_compare(0,
                        (ham_u8_t *)0,     0, (ham_u8_t *)"abc", 3));
    }

    void defaultPrefixCompareTest()
    {
        BFC_ASSERT(db_default_prefix_compare(0,
                        (ham_u8_t *)"abc", 3, 3, 
                        (ham_u8_t *)"abc", 3, 3)==HAM_PREFIX_REQUEST_FULLKEY);
        BFC_ASSERT(db_default_prefix_compare(0,
                        (ham_u8_t *)"ab",  2, 2, 
                        (ham_u8_t *)"abc", 3, 3)==-1); // comparison code has become 'smarter' so can resolve this one without the need for further help
		BFC_ASSERT(db_default_prefix_compare(0,
						(ham_u8_t *)"ab",  2, 3, 
						(ham_u8_t *)"abc", 3, 3)==HAM_PREFIX_REQUEST_FULLKEY);
        BFC_ASSERT(-1==db_default_prefix_compare(0,
                        (ham_u8_t *)"abc", 3, 3,
                        (ham_u8_t *)"bcd", 3, 3));
        BFC_ASSERT(db_default_prefix_compare(0,
                        (ham_u8_t *)"abc", 3, 3,
                        (ham_u8_t *)0,     0, 0)==+1); // comparison code has become 'smarter' so can resolve this one without the need for further help
        BFC_ASSERT(db_default_prefix_compare(0,
                        (ham_u8_t *)0,     0, 0,
                        (ham_u8_t *)"abc", 3, 3)==-1); // comparison code has become 'smarter' so can resolve this one without the need for further help
		BFC_ASSERT(db_default_prefix_compare(0,
						(ham_u8_t *)"abc", 3, 3,
						(ham_u8_t *)0,     0, 3)==HAM_PREFIX_REQUEST_FULLKEY);
		BFC_ASSERT(db_default_prefix_compare(0,
						(ham_u8_t *)0,     0, 3,
						(ham_u8_t *)"abc", 3, 3)==HAM_PREFIX_REQUEST_FULLKEY);
        BFC_ASSERT(db_default_prefix_compare(0,
                        (ham_u8_t *)"abc", 3, 80239, 
                        (ham_u8_t *)"abc", 3, 2)==HAM_PREFIX_REQUEST_FULLKEY);
    }

    void allocPageTest(void)
    {
        ham_page_t *page;
        BFC_ASSERT((page=db_alloc_page(m_db, 0, PAGE_IGNORE_FREELIST))!=0);
        BFC_ASSERT(page_get_owner(page)==m_db);
        BFC_ASSERT(db_free_page(page, 0)==HAM_SUCCESS);
        BFC_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
    }

    void fetchPageTest(void)
    {
        ham_page_t *p1, *p2;
        BFC_ASSERT((p1=db_alloc_page(m_db, 0, PAGE_IGNORE_FREELIST))!=0);
        BFC_ASSERT(page_get_owner(p1)==m_db);
        BFC_ASSERT((p2=db_fetch_page(m_db, page_get_self(p1), 0))!=0);
        BFC_ASSERT(page_get_self(p2)==page_get_self(p1));
        BFC_ASSERT(db_free_page(p1, 0)==HAM_SUCCESS);
        BFC_ASSERT(db_free_page(p2, 0)==HAM_SUCCESS);
    }

    void flushPageTest(void)
    {
        ham_page_t *page;
        ham_offset_t address;
        ham_u8_t *p;

        ham_cache_t *cache=cache_new(m_db, 15);
        BFC_ASSERT(cache!=0);
        db_set_cache(m_db, cache);

        BFC_ASSERT((page=db_alloc_page(m_db, 0, PAGE_IGNORE_FREELIST))!=0);
        BFC_ASSERT(page_get_owner(page)==m_db);
        p=page_get_raw_payload(page);
        for (int i=0; i<16; i++)
            p[i]=(ham_u8_t)i;
        page_set_dirty(page);
        address=page_get_self(page);
        BFC_ASSERT(db_flush_page(m_db, page, 0)==HAM_SUCCESS);
        BFC_ASSERT(db_free_page(page, 0)==HAM_SUCCESS);

        BFC_ASSERT((page=db_fetch_page(m_db, address, 0))!=0);
        BFC_ASSERT(page_get_self(page)==address);
        p=page_get_raw_payload(page);
        /* TODO see comment in db.c - db_free_page()
        for (int i=0; i<16; i++)
            BFC_ASSERT(p[i]==(ham_u8_t)i);
        */
        BFC_ASSERT(db_free_page(page, 0)==HAM_SUCCESS);
    }

	// using a function to compare the constants is easier for debugging
	bool compare_sizes(size_t a, size_t b)
	{
		return a == b;
	}

	void checkStructurePackingTest(void)
    {
		int i;

		// checks to make sure structure packing by the compiler is still okay
		// HAM_PACK_0 HAM_PACK_1 HAM_PACK_2 OFFSETOF
		BFC_ASSERT(compare_sizes(sizeof(ham_backend_t), OFFSETOF(ham_btree_t, _rootpage)));
		BFC_ASSERT(compare_sizes(sizeof(blob_t), 28));
		BFC_ASSERT(compare_sizes(sizeof(dupe_entry_t), 16));
		BFC_ASSERT(compare_sizes(sizeof(dupe_table_t), 8 + sizeof(dupe_entry_t)));
		BFC_ASSERT(compare_sizes(sizeof(ham_btree_t) - OFFSETOF(ham_btree_t, _rootpage), 8 + 2));
		BFC_ASSERT(compare_sizes(sizeof(btree_node_t), 28 + sizeof(int_key_t)));
		BFC_ASSERT(compare_sizes(sizeof(int_key_t), 12));
		BFC_ASSERT(compare_sizes(sizeof(db_header_t), 20));
		BFC_ASSERT(compare_sizes(sizeof(db_indexdata_t), 32));
		db_indexdata_t d;
		BFC_ASSERT(compare_sizes(sizeof(d.b), 32));
		BFC_ASSERT(compare_sizes(DB_INDEX_SIZE, 32));
		BFC_ASSERT(compare_sizes(sizeof(freelist_payload_t), 16 + 13 + sizeof(freelist_page_statistics_t)));
		freelist_payload_t f;
		BFC_ASSERT(compare_sizes(sizeof(f._s._s16), 5));
		BFC_ASSERT(compare_sizes(OFFSETOF(freelist_payload_t, _s._s16), 16));
		BFC_ASSERT(compare_sizes(OFFSETOF(freelist_payload_t, _s._s16._bitmap), 16 + 4));
		BFC_ASSERT(compare_sizes(sizeof(freelist_page_statistics_t), 4*8 + sizeof(freelist_slotsize_stats_t)*HAM_FREELIST_SLOT_SPREAD));
		BFC_ASSERT(compare_sizes(sizeof(freelist_slotsize_stats_t), 8*4));
		BFC_ASSERT(compare_sizes(HAM_FREELIST_SLOT_SPREAD, 16-5+1));
		BFC_ASSERT(compare_sizes(db_get_freelist_header_size16(), 16 + 4));
		BFC_ASSERT(compare_sizes(db_get_freelist_header_size32(), 16 + 12 + sizeof(freelist_page_statistics_t)));
		BFC_ASSERT(compare_sizes(db_get_int_key_header_size(), 11));
		BFC_ASSERT(compare_sizes(sizeof(log_header_t), 8));
		BFC_ASSERT(compare_sizes(sizeof(log_entry_t), 40));
		BFC_ASSERT(compare_sizes(sizeof(ham_perm_page_union_t), 13));
		ham_perm_page_union_t p;
		BFC_ASSERT(compare_sizes(sizeof(p._s), 13));
		BFC_ASSERT(compare_sizes(db_get_persistent_header_size(), 12));

		BFC_ASSERT(compare_sizes(OFFSETOF(btree_node_t, _entries), 28));
		ham_page_t page = {{0}};
		ham_db_t db = {0};
		ham_backend_t be = {0};

		page_set_self(&page, 1000);
		page_set_owner(&page, &db);
		db_set_backend(&db, &be);
		be_set_keysize(&be, 666);
		for (i = 0; i < 5; i++)
		{
			BFC_ASSERT_I(compare_sizes(btree_node_get_key_offset(&page, i), 1000+12+28+(i*(11+666))), i);
		}
		BFC_ASSERT(compare_sizes(db_get_persistent_header_size(), 12));
		// make sure the 'header page' is at least as large as your usual header page,
		// then hack it...
		struct
		{
			ham_perm_page_union_t drit;
			db_header_t drat;
		} hdrpage_pers = {{{0}}};
		ham_page_t hdrpage = {{0}};
		hdrpage._pers = (ham_perm_page_union_t *)&hdrpage_pers;
		db_set_header_page(&db, &hdrpage);
		ham_page_t *hp = db_get_header_page(&db);
		BFC_ASSERT(hp == (ham_page_t *)&hdrpage);
		ham_u8_t *pl1 = page_get_payload(hp);
		BFC_ASSERT(pl1);
		BFC_ASSERT(compare_sizes(pl1 - (ham_u8_t *)hdrpage._pers, 12));
		db_header_t *hdrptr = db_get_header(&db);
		BFC_ASSERT(compare_sizes(((ham_u8_t *)hdrptr) - (ham_u8_t *)hdrpage._pers, 12));
		db_set_max_databases(&db, 71);
		BFC_ASSERT(compare_sizes(SIZEOF_FULL_HEADER(&db), 20 + 71 * DB_INDEX_SIZE));
		BFC_ASSERT(compare_sizes(DB_INDEX_SIZE, 32));
    }

};

class DbInMemoryTest : public DbTest
{
public:
    DbInMemoryTest()
    :   DbTest(true, "DbInMemoryTest")
    {
        clear_tests(); // don't inherit tests
        testrunner::get_instance()->register_fixture(this);
		BFC_REGISTER_TEST(DbInMemoryTest, checkStructurePackingTest);
        BFC_REGISTER_TEST(DbInMemoryTest, headerTest);
        BFC_REGISTER_TEST(DbInMemoryTest, structureTest);
        BFC_REGISTER_TEST(DbInMemoryTest, envStructureTest);
        BFC_REGISTER_TEST(DbInMemoryTest, defaultCompareTest);
        BFC_REGISTER_TEST(DbInMemoryTest, defaultPrefixCompareTest);
        BFC_REGISTER_TEST(DbInMemoryTest, allocPageTest);
        //BFC_REGISTER_TEST(DbInMemoryTest, fetchPageTest);
        //BFC_REGISTER_TEST(DbInMemoryTest, flushPageTest);
    }
};

BFC_REGISTER_FIXTURE(DbTest);
BFC_REGISTER_FIXTURE(DbInMemoryTest);
