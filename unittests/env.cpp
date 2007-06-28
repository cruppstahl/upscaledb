/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * unit tests for mem.h/mem.c
 *
 */

#include <stdexcept>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/env.h"
#include "../src/cache.h"
#include "../src/page.h"
#include "memtracker.h"

class EnvTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EnvTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (newDeleteTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (createCloseOpenCloseTest);
    CPPUNIT_TEST      (openFailCloseTest);
    CPPUNIT_TEST_SUITE_END();

public:
    void setUp()
    { 
    }
    
    void tearDown() 
    { 
    }

    void structureTest()
    {
        ham_env_t *env;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT(env_get_txn_id(env)==0);
        env_set_txn_id(env, (ham_u64_t)14);
        CPPUNIT_ASSERT(env_get_txn_id(env)==(ham_u64_t)14);
        env_set_txn_id(env, 0);

        CPPUNIT_ASSERT(env_get_device(env)==0);
        env_set_device(env, (ham_device_t *)15);
        CPPUNIT_ASSERT(env_get_device(env)==(ham_device_t *)15);
        env_set_device(env, 0);

        CPPUNIT_ASSERT(env_get_cache(env)==0);
        env_set_cache(env, (ham_cache_t *)16);
        CPPUNIT_ASSERT(env_get_cache(env)==(ham_cache_t *)16);
        env_set_cache(env, 0);

        CPPUNIT_ASSERT(env_get_freelist_txn(env)==0);
        env_set_freelist_txn(env, (ham_txn_t *)17);
        CPPUNIT_ASSERT(env_get_freelist_txn(env)==(ham_txn_t *)17);
        env_set_freelist_txn(env, 0);

        CPPUNIT_ASSERT(env_get_header_page(env)==0);
        env_set_header_page(env, (ham_page_t *)18);
        CPPUNIT_ASSERT(env_get_header_page(env)==(ham_page_t *)18);
        env_set_header_page(env, 0);

        CPPUNIT_ASSERT(env_get_txn(env)==0);
        env_set_txn(env, (ham_txn_t *)19);
        CPPUNIT_ASSERT(env_get_txn(env)==(ham_txn_t *)19);
        env_set_txn(env, 0);

        CPPUNIT_ASSERT(env_get_extkey_cache(env)==0);
        env_set_extkey_cache(env, (extkey_cache_t *)20);
        CPPUNIT_ASSERT(env_get_extkey_cache(env)==(extkey_cache_t *)20);

        CPPUNIT_ASSERT(env_get_rt_flags(env)==0);
        env_set_rt_flags(env, 21);
        CPPUNIT_ASSERT(env_get_rt_flags(env)==21);

        CPPUNIT_ASSERT(env_get_list(env)==0);
        env_set_list(env, (ham_db_t *)22);
        CPPUNIT_ASSERT(env_get_list(env)==(ham_db_t *)22);

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void newDeleteTest(void)
    {
        ham_env_t *env;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseTest(void)
    {
        ham_env_t *env;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseOpenCloseTest(void)
    {
        ham_env_t *env;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void openFailCloseTest(void)
    {
        ham_env_t *env;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, 
                ham_env_open(env, "xxxxxx...", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(EnvTest);
