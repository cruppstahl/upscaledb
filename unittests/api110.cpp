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
#include "../src/statistics.h"
#include "../src/os.h"
#include "os.hpp"
#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;


class APIv110Test : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    APIv110Test()
    :   hamsterDB_fixture("APIv110Test"), m_db(NULL), m_alloc(NULL)
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(APIv110Test, transactionTest);
        BFC_REGISTER_TEST(APIv110Test, v10xDBformatDetectTest);
        BFC_REGISTER_TEST(APIv110Test, getEnvParamsTest);
        BFC_REGISTER_TEST(APIv110Test, getDbParamsTest);
    }

protected:
    ham_db_t *m_db;
    ham_env_t *m_env;
    memtracker_t *m_alloc;

public:
    virtual void setup()
    { 
        __super::setup();

        os::unlink(BFC_OPATH(".test"));
        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_env_new(&m_env));
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        //db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL(0, ham_create(m_db, 0, HAM_IN_MEMORY_DB, 0));
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_AUTO_CLEANUP));
        ham_delete(m_db);
        BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
        ham_env_delete(m_env);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void transactionTest(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, ham_txn_abort(txn, 0));

        // reopen the database, check the transaction flag vs. actual 
        // use of transactions
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        m_db=0;
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));

        BFC_ASSERT(ham_new(&m_db)==HAM_SUCCESS);
        //db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL(HAM_SUCCESS, 
                ham_create(m_db, BFC_OPATH(".test"), 
                    HAM_ENABLE_TRANSACTIONS, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        // can we cope with dual ham_close(), BTW? if not, we b0rk in teardown()
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    };

    void v10xDBformatDetectTest(void)
    {
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        os::unlink(BFC_OPATH(".test"));

        BFC_ASSERT_EQUAL(true, 
            os::copy(BFC_IPATH("data/dupe-endian-test-open-database-be.hdb"), 
                BFC_OPATH(".test")));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        os::unlink(BFC_OPATH(".test"));

        BFC_ASSERT_EQUAL(true, 
            os::copy(BFC_IPATH("data/dupe-endian-test-open-database-le.hdb"), 
                BFC_OPATH(".test")));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        os::unlink(BFC_OPATH(".test"));

        /* now the same, environment-based */
        BFC_ASSERT_EQUAL(true, 
            os::copy(BFC_IPATH("data/dupe-endian-test-open-database-be.hdb"), 
                BFC_OPATH(".test")));
        BFC_ASSERT_EQUAL(0, ham_env_open(m_env, BFC_OPATH(".test"), 0));

        ham_db_t *m_db2;
        BFC_ASSERT_EQUAL(0, ham_new(&m_db2)); 
        // first and only DB in there seems to be db # 0xF000, 
        // which is an illegal number
        BFC_ASSERT_EQUAL(0, 
                ham_env_open_db(m_env, m_db2, HAM_FIRST_DATABASE_NAME /* 1 */, 
                    0, 0));
        
        BFC_ASSERT_EQUAL(0, ham_close(m_db2, 0));
        ham_delete(m_db2);
        BFC_ASSERT_EQUAL(0, ham_env_close(m_env, 0));
        os::unlink(BFC_OPATH(".test"));

        BFC_ASSERT_EQUAL(true, 
            os::copy(BFC_IPATH("data/dupe-endian-test-open-database-le.hdb"), 
                BFC_OPATH(".test")));
        BFC_ASSERT_EQUAL(0, ham_env_open(m_env, BFC_OPATH(".test"), 0));
    }

    ham_offset_t get_param_value(ham_parameter_t *param, ham_u16_t name)
    {
        for (; param->name; param++) {
            if (param->name == name)
                return param->value;
        }
        return (ham_offset_t)-1;
    }

    void getEnvParamsTest(void)
    {
        ham_env_t *env;
        ham_statistics_t stats = {0};
        ham_parameter_t params0[] =
        {
            {HAM_PARAM_CACHESIZE, 0},
            {HAM_PARAM_KEYSIZE, 0},
            {HAM_PARAM_PAGESIZE, 0},
            {HAM_PARAM_MAX_ENV_DATABASES, 0},
            //{HAM_PARAM_DBNAME, 0},
            {HAM_PARAM_GET_FLAGS, 0},
            {HAM_PARAM_GET_FILEMODE, 0},
            {HAM_PARAM_GET_FILENAME, 0},
            {HAM_PARAM_GET_KEYS_PER_PAGE, 0},
            {HAM_PARAM_GET_STATISTICS, (ham_offset_t)&stats},
            {0,0}
        };

        ham_size_t sollwert_pagesize = os_get_pagesize();
        ham_size_t sollwert_cachesize = HAM_DEFAULT_CACHESIZE;
        ham_size_t sollwert_keysize = 21;
        ham_parameter_t *params = params0;

        BFC_ASSERT_EQUAL(0, ham_env_get_parameters(NULL, params));

        BFC_ASSERT_EQUAL(HAM_TRUE, stats.dont_collect_global_stats);
        BFC_ASSERT_EQUAL(HAM_TRUE, stats.dont_collect_db_stats);
        BFC_ASSERT_EQUAL(HAM_TRUE, stats.dont_collect_freelist_stats);
        BFC_ASSERT_EQUAL(sollwert_cachesize, 
                get_param_value(params, HAM_PARAM_CACHESIZE));
        BFC_ASSERT_EQUAL(sollwert_keysize, 
                get_param_value(params, HAM_PARAM_KEYSIZE));
        BFC_ASSERT_EQUAL(sollwert_pagesize, 
                get_param_value(params, HAM_PARAM_PAGESIZE));
        switch (sollwert_pagesize) {
            case 64*1024:
                BFC_ASSERT_EQUAL((ham_offset_t)2029, 
                        get_param_value(params, HAM_PARAM_MAX_ENV_DATABASES));
                BFC_ASSERT_EQUAL((ham_offset_t)2046, 
                        get_param_value(params, HAM_PARAM_GET_KEYS_PER_PAGE));
                break;
            case 16*1024:
                BFC_ASSERT_EQUAL((ham_offset_t)493, 
                        get_param_value(params, HAM_PARAM_MAX_ENV_DATABASES));
                BFC_ASSERT_EQUAL((ham_offset_t)510, 
                        get_param_value(params, HAM_PARAM_GET_KEYS_PER_PAGE));
                break;
            default:
                BFC_ASSERT(!"unknown pagesize");
        }
        BFC_ASSERT_EQUAL((ham_offset_t)-1, 
                get_param_value(params, HAM_PARAM_DBNAME)); // illegal search
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_FLAGS));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_FILEMODE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_FILENAME));
        BFC_ASSERT_EQUAL((ham_offset_t)&stats, 
                get_param_value(params, HAM_PARAM_GET_STATISTICS));

        /* now the same again but with an 'env' handle */
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));

        BFC_ASSERT_EQUAL(0, ham_env_get_parameters(NULL, params));

        BFC_ASSERT_EQUAL(HAM_TRUE, stats.dont_collect_global_stats);
        BFC_ASSERT_EQUAL(HAM_TRUE, stats.dont_collect_db_stats);
        BFC_ASSERT_EQUAL(HAM_TRUE, stats.dont_collect_freelist_stats);
        BFC_ASSERT_EQUAL(sollwert_cachesize, 
                get_param_value(params, HAM_PARAM_CACHESIZE));
        BFC_ASSERT_EQUAL(sollwert_keysize, 
                get_param_value(params, HAM_PARAM_KEYSIZE));
        BFC_ASSERT_EQUAL(sollwert_pagesize, 
                get_param_value(params, HAM_PARAM_PAGESIZE));
        switch (sollwert_pagesize) {
            case 64*1024:
                BFC_ASSERT_EQUAL((ham_offset_t)2029, 
                        get_param_value(params, HAM_PARAM_MAX_ENV_DATABASES));
                BFC_ASSERT_EQUAL((ham_offset_t)2046, 
                        get_param_value(params, HAM_PARAM_GET_KEYS_PER_PAGE));
                break;
            case 16*1024:
                BFC_ASSERT_EQUAL((ham_offset_t)493, 
                        get_param_value(params, HAM_PARAM_MAX_ENV_DATABASES));
                BFC_ASSERT_EQUAL((ham_offset_t)510, 
                        get_param_value(params, HAM_PARAM_GET_KEYS_PER_PAGE));
                break;
            default:
                BFC_ASSERT(!"unknown pagesize");
        }
        BFC_ASSERT_EQUAL((ham_offset_t)-1, 
                get_param_value(params, HAM_PARAM_DBNAME)); // illegal search
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_FLAGS));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_FILEMODE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_FILENAME));
        BFC_ASSERT_EQUAL((ham_offset_t)&stats, 
                get_param_value(params, HAM_PARAM_GET_STATISTICS));

        ham_env_delete(env);
    }

    void getDbParamsTest(void)
    {
        ham_db_t *db;
        ham_statistics_t stats = {0};
        ham_parameter_t params0[] =
        {
            {HAM_PARAM_CACHESIZE, 0},
            {HAM_PARAM_KEYSIZE, 0},
            {HAM_PARAM_PAGESIZE, 0},
            {HAM_PARAM_MAX_ENV_DATABASES, 0},
            {HAM_PARAM_DBNAME, 0},
            {HAM_PARAM_GET_FLAGS, 0},
            {HAM_PARAM_GET_FILEMODE, 0},
            {HAM_PARAM_GET_FILENAME, 0},
            {HAM_PARAM_GET_KEYS_PER_PAGE, 0},
            {HAM_PARAM_GET_DAM, 0},
            {HAM_PARAM_GET_STATISTICS, (ham_offset_t)&stats},
            {0,0}
        };
        ham_parameter_t params1[] =
        {
            {HAM_PARAM_CACHESIZE, 0},
            {HAM_PARAM_KEYSIZE, 0},
            {HAM_PARAM_PAGESIZE, 0},
            {HAM_PARAM_MAX_ENV_DATABASES, 0},
            {HAM_PARAM_DBNAME, 0},
            {HAM_PARAM_GET_FLAGS, 0},
            {HAM_PARAM_GET_FILEMODE, 0},
            {HAM_PARAM_GET_FILENAME, 0},
            {HAM_PARAM_GET_KEYS_PER_PAGE, 0},
            {HAM_PARAM_GET_STATISTICS, (ham_offset_t)&stats},
            {0,0}
        };

        ham_size_t sollwert_pagesize = os_get_pagesize();
        ham_size_t sollwert_cachesize = HAM_DEFAULT_CACHESIZE;
        ham_size_t sollwert_keysize = 21;
        ham_parameter_t *params = params0;

        BFC_ASSERT_EQUAL(0, ham_get_parameters(NULL, params));
        BFC_ASSERT_EQUAL(HAM_TRUE, stats.dont_collect_global_stats);
        BFC_ASSERT_EQUAL(HAM_TRUE, stats.dont_collect_db_stats);
        BFC_ASSERT_EQUAL(HAM_TRUE, stats.dont_collect_freelist_stats);
        BFC_ASSERT_EQUAL(sollwert_cachesize, 
                get_param_value(params, HAM_PARAM_CACHESIZE));
        BFC_ASSERT_EQUAL(sollwert_keysize, 
                get_param_value(params, HAM_PARAM_KEYSIZE));
        BFC_ASSERT_EQUAL(sollwert_pagesize, 
                get_param_value(params, HAM_PARAM_PAGESIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_DAM));
        switch (sollwert_pagesize) {
            case 64*1024:
                BFC_ASSERT_EQUAL((ham_offset_t)2029,
                        get_param_value(params, HAM_PARAM_MAX_ENV_DATABASES));
                BFC_ASSERT_EQUAL((ham_offset_t)2046,
                        get_param_value(params, HAM_PARAM_GET_KEYS_PER_PAGE));
                break;
            case 16*1024:
                BFC_ASSERT_EQUAL((ham_offset_t)493,
                        get_param_value(params, HAM_PARAM_MAX_ENV_DATABASES));
                BFC_ASSERT_EQUAL((ham_offset_t)510,
                        get_param_value(params, HAM_PARAM_GET_KEYS_PER_PAGE));
                break;
            default:
                BFC_ASSERT(!"unknown pagesize");
        }
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_DBNAME));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_FLAGS));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_FILEMODE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_FILENAME));
        BFC_ASSERT_EQUAL((ham_offset_t)&stats, 
                get_param_value(params, HAM_PARAM_GET_STATISTICS));

        ham_new(&db);

        sollwert_pagesize = os_get_pagesize();
        sollwert_cachesize = HAM_DEFAULT_CACHESIZE;
        sollwert_keysize = 21;
        params = params1;

        BFC_ASSERT_EQUAL(0, ham_get_parameters(db, params));
        BFC_ASSERT_EQUAL(HAM_TRUE, stats.dont_collect_global_stats);
        BFC_ASSERT_EQUAL(HAM_TRUE, stats.dont_collect_db_stats);
        BFC_ASSERT_EQUAL(HAM_TRUE, stats.dont_collect_freelist_stats);
        BFC_ASSERT_EQUAL(sollwert_cachesize, 
                get_param_value(params, HAM_PARAM_CACHESIZE));
        BFC_ASSERT_EQUAL(sollwert_keysize, 
                get_param_value(params, HAM_PARAM_KEYSIZE));
        BFC_ASSERT_EQUAL(sollwert_pagesize, 
                get_param_value(params, HAM_PARAM_PAGESIZE));
        switch (sollwert_pagesize) {
            case 64*1024:
                BFC_ASSERT_EQUAL((ham_offset_t)2029,
                        get_param_value(params, HAM_PARAM_MAX_ENV_DATABASES));
                BFC_ASSERT_EQUAL((ham_offset_t)2046,
                        get_param_value(params, HAM_PARAM_GET_KEYS_PER_PAGE));
                break;
            case 16*1024:
                BFC_ASSERT_EQUAL((ham_offset_t)493,
                        get_param_value(params, HAM_PARAM_MAX_ENV_DATABASES));
                BFC_ASSERT_EQUAL((ham_offset_t)510,
                        get_param_value(params, HAM_PARAM_GET_KEYS_PER_PAGE));
                break;
            default:
                BFC_ASSERT(!"unknown pagesize");
        }
        BFC_ASSERT_EQUAL((ham_offset_t)HAM_FIRST_DATABASE_NAME, 
                get_param_value(params, HAM_PARAM_DBNAME));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_FLAGS));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_FILEMODE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                get_param_value(params, HAM_PARAM_GET_FILENAME));
        BFC_ASSERT_EQUAL((ham_offset_t)&stats, 
                get_param_value(params, HAM_PARAM_GET_STATISTICS));

        ham_delete(db);
    }
};


BFC_REGISTER_FIXTURE(APIv110Test);

