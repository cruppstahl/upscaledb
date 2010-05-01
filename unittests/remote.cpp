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
        BFC_REGISTER_TEST(RemoteTest, autoCleanupTest);
        BFC_REGISTER_TEST(RemoteTest, autoCleanup2Test);
        BFC_REGISTER_TEST(RemoteTest, getDatabaseNamesTest);
    }

protected:
    ham_env_t *m_srvenv;
    hamserver_t *m_srv;

    void setup(void)
    {
        hamserver_config_t config;
        config.port=8686;
        BFC_ASSERT_EQUAL(HAM_TRUE, 
                hamserver_init(&config, &m_srv));

        BFC_ASSERT_EQUAL(0, ham_env_new(&m_srvenv));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(m_srvenv, "test.db", 0, 0664));

        BFC_ASSERT_EQUAL(HAM_TRUE, 
                hamserver_add_env(m_srv, m_srvenv, "/test"));
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

        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, "http://localhost:77/test.db", 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void invalidPathTest(void)
    {
        ham_env_t *env;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));

        BFC_ASSERT_EQUAL(0, 
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
                ham_env_create(env, "http://localhost:8989/test.db", 0, 0664));
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
                ham_env_create(env, "http://localhost:8989/test.db", 0, 0664));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        
        BFC_ASSERT_EQUAL(0u, env_is_active(env));
        BFC_ASSERT_EQUAL(0,
            ham_env_open(env, "http://localhost:8989/test.db", 0));
        BFC_ASSERT_EQUAL(1u, env_is_active(env));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0u, env_is_active(env));

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
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

    void getDatabaseNamesTest(void)
    {
#if 0
        ham_env_t *env;
        ham_db_t *db1, *db2, *db3;
        ham_u16_t names[5];
        ham_size_t names_size=0;

        BFC_ASSERT_EQUAL(0, ham_new(&db1));
        BFC_ASSERT_EQUAL(0, ham_new(&db2));
        BFC_ASSERT_EQUAL(0, ham_new(&db3));
        BFC_ASSERT_EQUAL(0, ham_env_new(&env));

        BFC_ASSERT_EQUAL(0, ham_env_create(env, BFC_OPATH(".test"), 0, 0664));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                        ham_env_get_database_names(0, names, &names_size));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                        ham_env_get_database_names(env, 0, &names_size));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                        ham_env_get_database_names(env, names, 0));

        names_size=1;
        BFC_ASSERT_EQUAL(0,
                        ham_env_get_database_names(env, names, &names_size));
        BFC_ASSERT_EQUAL((ham_size_t)0, names_size);

        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db1, 111, 0, 0));
        names_size=0;
        BFC_ASSERT_EQUAL(HAM_LIMITS_REACHED,
                        ham_env_get_database_names(env, names, &names_size));

        names_size=1;
        BFC_ASSERT_EQUAL(0,
                        ham_env_get_database_names(env, names, &names_size));
        BFC_ASSERT_EQUAL((ham_size_t)1, names_size);
        BFC_ASSERT_EQUAL((ham_u16_t)111, names[0]);

        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db2, 222, 0, 0));
        names_size=1;
        BFC_ASSERT_EQUAL(HAM_LIMITS_REACHED,
                        ham_env_get_database_names(env, names, &names_size));

        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db3, 333, 0, 0));
        names_size=5;
        BFC_ASSERT_EQUAL(0,
                        ham_env_get_database_names(env, names, &names_size));
        BFC_ASSERT_EQUAL((ham_size_t)3, names_size);
        BFC_ASSERT_EQUAL((ham_u16_t)111, names[0]);
        BFC_ASSERT_EQUAL((ham_u16_t)222, names[1]);
        BFC_ASSERT_EQUAL((ham_u16_t)333, names[2]);

        BFC_ASSERT_EQUAL(0, ham_close(db2, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_env_erase_db(env, 222, 0));
        names_size=5;
        BFC_ASSERT_EQUAL(0,
                    ham_env_get_database_names(env, names, &names_size));
        BFC_ASSERT_EQUAL((ham_size_t)2, names_size);
        BFC_ASSERT_EQUAL((ham_u16_t)111, names[0]);
        BFC_ASSERT_EQUAL((ham_u16_t)333, names[1]);

        BFC_ASSERT_EQUAL(0, ham_close(db1, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db3, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(db1));
        BFC_ASSERT_EQUAL(0, ham_delete(db2));
        BFC_ASSERT_EQUAL(0, ham_delete(db3));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
#endif
    }

};

BFC_REGISTER_FIXTURE(RemoteTest);
