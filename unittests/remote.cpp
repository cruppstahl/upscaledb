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
#include <cstdlib>
#include <ham/hamsterdb_int.h>
#include "memtracker.h"
#include "../src/env.h"
#include "../server/hamserver.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

#define SERVER_URL "http://localhost:8080/test.db"

class RemoteTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    RemoteTest()
    :   hamsterDB_fixture("RemoteTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(RemoteTest, invalidUrlTest);
        BFC_REGISTER_TEST(RemoteTest, invalidPathTest);
        BFC_REGISTER_TEST(RemoteTest, createCloseTest);
        BFC_REGISTER_TEST(RemoteTest, createCloseOpenCloseTest);
        BFC_REGISTER_TEST(RemoteTest, getEnvParamsTest);
        BFC_REGISTER_TEST(RemoteTest, getDatabaseNamesTest);
        BFC_REGISTER_TEST(RemoteTest, envFlushTest);
        BFC_REGISTER_TEST(RemoteTest, renameDbTest);
        BFC_REGISTER_TEST(RemoteTest, autoCleanupTest);
        BFC_REGISTER_TEST(RemoteTest, autoCleanup2Test);
    }

protected:
    ham_env_t *m_srvenv;
    hamserver_t *m_srv;

    void setup(void)
    {
        hamserver_config_t config;
        config.port=8989;
        BFC_ASSERT_EQUAL(HAM_TRUE, 
                hamserver_init(&config, &m_srv));

        BFC_ASSERT_EQUAL(0, ham_env_new(&m_srvenv));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(m_srvenv, "test.db", 0, 0664));

        BFC_ASSERT_EQUAL(HAM_TRUE, 
                hamserver_add_env(m_srv, m_srvenv, "/test.db"));
    }

    void teardown(void)
    {
        hamserver_close(m_srv);
        ham_env_close(m_srvenv, 0);
        ham_env_delete(m_srvenv);
    }

    void invalidUrlTest(void)
    {
        ham_env_t *env;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));

        BFC_ASSERT_EQUAL(HAM_NETWORK_ERROR, 
                ham_env_create(env, "http://localhost:77/test.db", 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void invalidPathTest(void)
    {
        ham_env_t *env;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));

        BFC_ASSERT_EQUAL(HAM_NETWORK_ERROR, 
                ham_env_create(env, "http://localhost:8989/xxxtest.db", 0, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseTest(void)
    {
        ham_env_t *env;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0u, env_is_active(env));

        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, SERVER_URL, 0, 0664));
        BFC_ASSERT_EQUAL(1u, env_is_active(env));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_env_close(0, 0));
        BFC_ASSERT_EQUAL(1u, env_is_active(env));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0u, env_is_active(env));

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseOpenCloseTest(void)
    {
        ham_env_t *env;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));

        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, SERVER_URL, 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        
        BFC_ASSERT_EQUAL(0u, env_is_active(env));
        BFC_ASSERT_EQUAL(0,
            ham_env_open(env, SERVER_URL, 0));
        BFC_ASSERT_EQUAL(1u, env_is_active(env));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0u, env_is_active(env));

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void getEnvParamsTest(void)
    {
        ham_env_t *env;
        ham_parameter_t params[] =
        {
            {HAM_PARAM_CACHESIZE, 0},
            {HAM_PARAM_PAGESIZE, 0},
            {HAM_PARAM_MAX_ENV_DATABASES, 0},
            {HAM_PARAM_GET_FLAGS, 0},
            {HAM_PARAM_GET_FILEMODE, 0},
            {HAM_PARAM_GET_FILENAME, 0},
            {0,0}
        };

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, SERVER_URL, 0, 0664));

        BFC_ASSERT_EQUAL(0, ham_env_get_parameters(env, params));

        BFC_ASSERT_EQUAL((unsigned)HAM_DEFAULT_CACHESIZE, params[0].value);
        BFC_ASSERT_EQUAL(1024*16u, params[1].value);
        BFC_ASSERT_EQUAL((ham_offset_t)16, params[2].value);
        BFC_ASSERT_EQUAL(640u, params[3].value);
        BFC_ASSERT_EQUAL((ham_offset_t)420, params[4].value);
        BFC_ASSERT_EQUAL(0, strcmp("test.db", (char *)params[5].value));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        ham_env_delete(env);
    }

    void getDatabaseNamesTest(void)
    {
        ham_env_t *env;
        ham_u16_t names[15];
        ham_size_t max_names=15;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, SERVER_URL, 0, 0664));

        BFC_ASSERT_EQUAL(0, 
                ham_env_get_database_names(env, &names[0], &max_names));

        //BFC_ASSERT_EQUAL(HAM_DEFAULT_DATABASE_NAME, names[0]);
        BFC_ASSERT_EQUAL(13, names[0]);
        BFC_ASSERT_EQUAL(1u, max_names);

        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        ham_env_delete(env);
    }

    void envFlushTest(void)
    {
        ham_env_t *env;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, SERVER_URL, 0, 0664));

        BFC_ASSERT_EQUAL(0, ham_env_flush(env, 0));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        ham_env_delete(env);
    }

    void renameDbTest(void)
    {
        ham_env_t *env;
        ham_u16_t names[15];
        ham_size_t max_names=15;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, SERVER_URL, 0, 0664));

        BFC_ASSERT_EQUAL(0, ham_env_rename_db(env, 13, 15, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_env_get_database_names(env, &names[0], &max_names));
        BFC_ASSERT_EQUAL(15, names[0]);
        BFC_ASSERT_EQUAL(1u, max_names);

        BFC_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND, 
                    ham_env_rename_db(env, 14, 16, 0));
        BFC_ASSERT_EQUAL(0, ham_env_rename_db(env, 15, 13, 0));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        ham_env_delete(env);
    }

    void autoCleanupTest(void)
    {
#if 0
        ham_env_t *env;
        ham_db_t *db[3];
        ham_cursor_t *c[5];

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        for (int i=0; i<3; i++)
            BFC_ASSERT_EQUAL(0, ham_new(&db[i]));

        BFC_ASSERT_EQUAL(0, ham_env_create(env, BFC_OPATH(".test"), 0, 0664));
        for (int i=0; i<3; i++)
            BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], i+1, 0, 0));
        for (int i=0; i<5; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_create(db[0], 0, 0, &c[i]));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
        for (int i=0; i<3; i++)
            BFC_ASSERT_EQUAL(0, ham_delete(db[i]));
#endif
    }

    void autoCleanup2Test(void)
    {
#if 0
        ham_env_t *env;
        ham_db_t *db;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, ham_new(&db));

        BFC_ASSERT_EQUAL(0, ham_env_create(env, BFC_OPATH(".test"), 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_create_db(env, db, 1, 0, 0));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));

        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
#endif
    }

};

BFC_REGISTER_FIXTURE(RemoteTest);
