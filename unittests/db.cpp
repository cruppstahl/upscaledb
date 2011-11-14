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
#include "../src/btree.h"
#include "../src/blob.h"
#include "../src/txn.h"
#include "../src/log.h"
#include "../src/freelist.h"
#include "memtracker.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;


class DbTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    DbTest(bool inmemory=false, const char *name="DbTest")
    :   hamsterDB_fixture(name),
        m_db(0), m_dbp(0), m_env(0), m_inmemory(inmemory), m_alloc(0)
    {
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
    Database *m_dbp;
    ham_env_t *m_env;
    ham_bool_t m_inmemory;
    memtracker_t *m_alloc;

public:
    virtual void setup() 
    { 
        __super::setup();

        m_alloc=memtracker_new();
        BFC_ASSERT_EQUAL(0, ham_env_new(&m_env));
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        env_set_allocator(m_env, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(m_env, BFC_OPATH(".test"), 
                        (m_inmemory ? HAM_IN_MEMORY_DB : 0), 0644));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(m_env, m_db, 13, 
                        HAM_ENABLE_DUPLICATES, 0));
        m_dbp=(Database *)m_db;
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        ham_env_close(m_env, 0);
        ham_close(m_db, 0);
        ham_delete(m_db);
        ham_env_delete(m_env);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void headerTest()
    {
        env_set_magic(m_env, '1', '2', '3', '4');
        env_header_t *hdr=env_get_header(m_env);
        BFC_ASSERT(env_get_magic(hdr, 0)=='1');
        BFC_ASSERT(env_get_magic(hdr, 1)=='2');
        BFC_ASSERT(env_get_magic(hdr, 2)=='3');
        BFC_ASSERT(env_get_magic(hdr, 3)=='4');

        env_set_version(m_env, 1, 2, 3, 4);
        BFC_ASSERT(env_get_version(m_env, 0)==1);
        BFC_ASSERT(env_get_version(m_env, 1)==2);
        BFC_ASSERT(env_get_version(m_env, 2)==3);
        BFC_ASSERT(env_get_version(m_env, 3)==4);

        env_set_serialno(m_env, 0x1234);
        BFC_ASSERT(env_get_serialno(m_env)==0x1234);
    }

    void structureTest()
    {
        BFC_ASSERT(env_get_header_page(m_env)!=0);

        BFC_ASSERT_EQUAL(0, m_dbp->get_error());
        m_dbp->set_error(HAM_IO_ERROR);
        BFC_ASSERT_EQUAL(HAM_IO_ERROR, m_dbp->get_error());

        BFC_ASSERT_NOTNULL(m_dbp->get_backend());// already initialized
        ham_backend_t *oldbe=m_dbp->get_backend();
        m_dbp->set_backend((ham_backend_t *)15);
        BFC_ASSERT_EQUAL((ham_backend_t *)15, m_dbp->get_backend());
        m_dbp->set_backend(oldbe);

        BFC_ASSERT_NOTNULL(env_get_cache(m_env));

        BFC_ASSERT(0!=m_dbp->get_prefix_compare_func());
        ham_prefix_compare_func_t oldfoo=m_dbp->get_prefix_compare_func();
        m_dbp->set_prefix_compare_func((ham_prefix_compare_func_t)18);
        BFC_ASSERT_EQUAL((ham_prefix_compare_func_t)18, 
                m_dbp->get_prefix_compare_func());
        m_dbp->set_prefix_compare_func(oldfoo);

        ham_compare_func_t oldfoo2=m_dbp->get_compare_func();
        BFC_ASSERT(0!=m_dbp->get_compare_func());
        m_dbp->set_compare_func((ham_compare_func_t)19);
        BFC_ASSERT_EQUAL((ham_compare_func_t)19, m_dbp->get_compare_func());
        m_dbp->set_compare_func(oldfoo2);

        page_set_undirty(env_get_header_page(m_env));
        BFC_ASSERT(!env_is_dirty(m_env));
        env_set_dirty(m_env);
        BFC_ASSERT(env_is_dirty(m_env));

        BFC_ASSERT(0!=m_dbp->get_rt_flags());

        BFC_ASSERT(m_dbp->get_env()!=0);

        BFC_ASSERT_EQUAL((void *)0, m_dbp->get_next());
        m_dbp->set_next((Database *)40);
        BFC_ASSERT_EQUAL((Database *)40, m_dbp->get_next());
        m_dbp->set_next((Database *)0);

        BFC_ASSERT_EQUAL(0u, m_dbp->get_record_allocsize());
        m_dbp->set_record_allocsize(21);
        BFC_ASSERT_EQUAL(21u, m_dbp->get_record_allocsize());
        m_dbp->set_record_allocsize(0);

        BFC_ASSERT_EQUAL((void *)0, m_dbp->get_record_allocdata());
        m_dbp->set_record_allocdata((void *)22);
        BFC_ASSERT_EQUAL((void *)22, m_dbp->get_record_allocdata());
        m_dbp->set_record_allocdata(0);

        BFC_ASSERT_EQUAL(1u, m_dbp->is_active());
        m_dbp->set_active(HAM_FALSE);
        BFC_ASSERT_EQUAL(0u, m_dbp->is_active());
        m_dbp->set_active(HAM_TRUE);
        BFC_ASSERT_EQUAL(1u, m_dbp->is_active());
    }

    void envStructureTest()
    {
        ham_env_t *env;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        env_set_txn_id(env, 0x12345ull);
        env_set_file_mode(env, 0666);
        env_set_device(env, (ham_device_t *)0x13);
        env_set_cache(env, (Cache *)0x14);
        env_set_rt_flags(env, 0x18);

        BFC_ASSERT_EQUAL((Cache *)0x14, env_get_cache(env));
        /* TODO test other stuff! */

        BFC_ASSERT_EQUAL(0u, env_is_active(env));
        env_set_active(env, HAM_TRUE);
        BFC_ASSERT_EQUAL(1u, env_is_active(env));
        env_set_active(env, HAM_FALSE);
        BFC_ASSERT_EQUAL(0u, env_is_active(env));

        env_set_device(env, 0);
        env_set_cache(env, 0);
        env_set_rt_flags(env, 0x18);
        env_set_header_page(env, 0);
        env_set_list(env, 0);
        env_set_header_page(env, (ham_page_t *)0);
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
        BFC_ASSERT_EQUAL(0,
                db_alloc_page(&page, m_dbp, 0, PAGE_IGNORE_FREELIST));
        BFC_ASSERT_EQUAL(m_dbp, page_get_owner(page));
        BFC_ASSERT_EQUAL(0, db_free_page(page, 0));
    }

    void fetchPageTest(void)
    {
        ham_page_t *p1, *p2;
        BFC_ASSERT_EQUAL(0,
                db_alloc_page(&p1, m_dbp, 0, PAGE_IGNORE_FREELIST));
        BFC_ASSERT_EQUAL(m_dbp, page_get_owner(p1));
        BFC_ASSERT_EQUAL(0,
                db_fetch_page(&p2, m_dbp, page_get_self(p1), 0));
        BFC_ASSERT_EQUAL(page_get_self(p2), page_get_self(p1));
        BFC_ASSERT_EQUAL(0, db_free_page(p1, 0));
    }

    void flushPageTest(void)
    {
        ham_page_t *page;
        ham_offset_t address;
        ham_u8_t *p;

        BFC_ASSERT_EQUAL(0,
                db_alloc_page(&page, m_dbp, 0, PAGE_IGNORE_FREELIST));

        BFC_ASSERT(page_get_owner(page)==m_dbp);
        p=page_get_raw_payload(page);
        for (int i=0; i<16; i++)
            p[i]=(ham_u8_t)i;
        page_set_dirty(page);
        address=page_get_self(page);
        BFC_ASSERT_EQUAL(0, db_flush_page(m_env, page));
        BFC_ASSERT_EQUAL(0, db_free_page(page, 0));

        BFC_ASSERT_EQUAL(0, db_fetch_page(&page, m_dbp, address, 0));
        BFC_ASSERT(page!=0);
        BFC_ASSERT_EQUAL(address, page_get_self(page));
        p=page_get_raw_payload(page);
        /* TODO see comment in db.c - db_free_page()
        for (int i=0; i<16; i++)
            BFC_ASSERT(p[i]==(ham_u8_t)i);
        */
        BFC_ASSERT_EQUAL(0, db_free_page(page, 0));
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
        BFC_ASSERT(compare_sizes(sizeof(ham_backend_t), 
                OFFSETOF(ham_btree_t, _rootpage)));
        BFC_ASSERT(compare_sizes(sizeof(blob_t), 28));
        BFC_ASSERT(compare_sizes(sizeof(dupe_entry_t), 16));
        BFC_ASSERT(compare_sizes(sizeof(dupe_table_t), 
                8 + sizeof(dupe_entry_t)));
        BFC_ASSERT(compare_sizes(sizeof(ham_btree_t) - OFFSETOF(ham_btree_t, 
                _rootpage), 8 + sizeof(void *)*2 + 2));
        BFC_ASSERT(compare_sizes(sizeof(btree_node_t), 28+sizeof(btree_key_t)));
        BFC_ASSERT(compare_sizes(sizeof(btree_key_t), 12));
        BFC_ASSERT(compare_sizes(sizeof(env_header_t), 20));
        BFC_ASSERT(compare_sizes(sizeof(db_indexdata_t), 32));
        db_indexdata_t d;
        BFC_ASSERT(compare_sizes(sizeof(d.b), 32));
        BFC_ASSERT(compare_sizes(DB_INDEX_SIZE, 32));
        BFC_ASSERT(compare_sizes(sizeof(freelist_payload_t), 
                16 + 13 + sizeof(freelist_page_statistics_t)));
        freelist_payload_t f;
        BFC_ASSERT(compare_sizes(sizeof(f._s._s16), 5));
        BFC_ASSERT(compare_sizes(OFFSETOF(freelist_payload_t, 
                _s._s16), 16));
        BFC_ASSERT(compare_sizes(OFFSETOF(freelist_payload_t, 
                _s._s16._bitmap), 16 + 4));
        BFC_ASSERT(compare_sizes(sizeof(freelist_page_statistics_t), 
              4*8+sizeof(freelist_slotsize_stats_t)*HAM_FREELIST_SLOT_SPREAD));
        BFC_ASSERT(compare_sizes(sizeof(freelist_slotsize_stats_t), 8*4));
        BFC_ASSERT(compare_sizes(HAM_FREELIST_SLOT_SPREAD, 16-5+1));
        BFC_ASSERT(compare_sizes(db_get_freelist_header_size16(), 16 + 4));
        BFC_ASSERT(compare_sizes(db_get_freelist_header_size32(), 
                16 + 12 + sizeof(freelist_page_statistics_t)));
        BFC_ASSERT(compare_sizes(db_get_int_key_header_size(), 11));
        BFC_ASSERT(compare_sizes(sizeof(Log::Header), 16));
        BFC_ASSERT(compare_sizes(sizeof(Log::Entry), 32));
        BFC_ASSERT(compare_sizes(sizeof(ham_perm_page_union_t), 13));
        ham_perm_page_union_t p;
        BFC_ASSERT(compare_sizes(sizeof(p._s), 13));
        BFC_ASSERT(compare_sizes(page_get_persistent_header_size(), 12));

        BFC_ASSERT(compare_sizes(OFFSETOF(btree_node_t, _entries), 28));
        ham_page_t page = {{0}};
        Database db;
        ham_backend_t be = {0};

        page_set_self(&page, 1000);
        page_set_owner(&page, &db);
        db.set_backend(&be);
        be_set_keysize(&be, 666);
        for (i = 0; i < 5; i++) {
            BFC_ASSERT_I(compare_sizes(
                (ham_size_t)btree_node_get_key_offset(&page, i), 
                (ham_size_t)1000+12+28+(i*(11+666))), i);
        }
        BFC_ASSERT(compare_sizes(page_get_persistent_header_size(), 12));
        // make sure the 'header page' is at least as large as your usual 
        // header page, then hack it...
        struct
        {
            ham_perm_page_union_t drit;
            env_header_t drat;
        } hdrpage_pers = {{{0}}};
        ham_page_t hdrpage = {{0}};
        hdrpage._pers = (ham_perm_page_union_t *)&hdrpage_pers;
        ham_page_t *hp = &hdrpage;
        ham_u8_t *pl1 = page_get_payload(hp);
        BFC_ASSERT(pl1);
        BFC_ASSERT(compare_sizes(pl1 - (ham_u8_t *)hdrpage._pers, 12));
        env_header_t *hdrptr = (env_header_t *)(page_get_payload(&hdrpage));
        BFC_ASSERT(compare_sizes(((ham_u8_t *)hdrptr) - (ham_u8_t *)hdrpage._pers, 12));
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
    }
};

BFC_REGISTER_FIXTURE(DbTest);
BFC_REGISTER_FIXTURE(DbInMemoryTest);
