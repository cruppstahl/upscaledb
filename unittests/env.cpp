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
#include "../src/env.h"
#include "../src/cache.h"
#include "../src/page.h"
#include "../src/freelist.h"
#include "../src/db.h"
#include "memtracker.h"

class EnvTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EnvTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (newDeleteTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (createCloseOpenCloseTest);
    CPPUNIT_TEST      (createCloseOpenCloseWithDatabasesTest);
    CPPUNIT_TEST      (envNotEmptyTest);
    CPPUNIT_TEST      (autoCleanupTest);
    CPPUNIT_TEST      (readOnlyTest);
    CPPUNIT_TEST      (createPagesizeReopenTest);
    CPPUNIT_TEST      (openFailCloseTest);
    CPPUNIT_TEST      (openWithKeysizeTest);
    CPPUNIT_TEST      (createWithKeysizeTest);
    CPPUNIT_TEST      (createDbWithKeysizeTest);
    CPPUNIT_TEST      (disableVarkeyTests);
    CPPUNIT_TEST      (multiDbTest);
    CPPUNIT_TEST      (multiDbTest2);
    CPPUNIT_TEST      (multiDbInsertFindTest);
    CPPUNIT_TEST      (multiDbInsertFindExtendedTest);
    CPPUNIT_TEST      (multiDbInsertFindExtendedEraseTest);
    CPPUNIT_TEST      (multiDbInsertCursorTest);
    CPPUNIT_TEST      (multiDbInsertFindExtendedCloseReopenTest);
    CPPUNIT_TEST      (renameOpenDatabases);
    CPPUNIT_TEST      (renameClosedDatabases);
    CPPUNIT_TEST      (eraseOpenDatabases);
    CPPUNIT_TEST      (eraseUnknownDatabases);
    CPPUNIT_TEST      (eraseMultipleDatabases);
    CPPUNIT_TEST      (endianTestOpenDatabase);
    CPPUNIT_TEST      (limitsReachedTest);
    CPPUNIT_TEST      (createEnvOpenDbTest);
    CPPUNIT_TEST      (createFullEnvOpenDbTest);
    CPPUNIT_TEST      (createFullEnvOpenSecondDbTest);
    CPPUNIT_TEST      (getDatabaseNamesTest);
    CPPUNIT_TEST      (maxDatabasesTest);
    CPPUNIT_TEST      (maxDatabasesReopenTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_u32_t m_flags;

public:
    EnvTest(ham_u32_t flags=0)
    :   m_flags(flags)
    {
    }

    void setUp()
    { 
#if WIN32
        (void)DeleteFileA((LPCSTR)".test");
#else
        (void)unlink(".test");
#endif
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

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseOpenCloseTest(void)
    {
        ham_env_t *env;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createCloseOpenCloseWithDatabasesTest(void)
    {
        ham_env_t *env;
        ham_db_t *db;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
    }

    void envNotEmptyTest(void)
    {
        ham_env_t *env;
        ham_db_t *db;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_ENV_NOT_EMPTY, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
    }

    void autoCleanupTest(void)
    {
        ham_env_t *env;
        ham_db_t *db[3];
        ham_cursor_t *c[5];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        for (int i=0; i<3; i++)
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        for (int i=0; i<3; i++)
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], i+1, 0, 0));
        for (int i=0; i<5; i++)
            CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(db[0], 0, 0, &c[i]));

        CPPUNIT_ASSERT_EQUAL(HAM_ENV_NOT_EMPTY, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        for (int i=0; i<3; i++)
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
    }

    void readOnlyTest(void)
    {
        ham_db_t *db;
        ham_env_t *env;
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *cursor;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", 0, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, ".test", HAM_READ_ONLY));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db, 333, 0, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));
        CPPUNIT_ASSERT_EQUAL(HAM_DB_READ_ONLY, 
                ham_env_create_db(env, db, 444, 0, 0));

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
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        ham_delete(db);
        ham_env_delete(env);
    }

    void createPagesizeReopenTest(void)
    {
        ham_env_t *env;
        ham_parameter_t ps[]={{HAM_PARAM_PAGESIZE,   1024*128}, {0, 0}};

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_create_ex(env, ".test", m_flags, 0644, &ps[0]));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_open(env, ".test", m_flags));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void openFailCloseTest(void)
    {
        ham_env_t *env;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(HAM_FILE_NOT_FOUND, 
                ham_env_open(env, "xxxxxx...", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void openWithKeysizeTest(void)
    {
        ham_env_t *env;
        ham_parameter_t parameters[]={
           { HAM_PARAM_KEYSIZE,      (ham_u64_t)20 },
           { 0, 0ull }
        };

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_env_open_ex(env, ".test", m_flags, &parameters[0]));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createWithKeysizeTest(void)
    {
        ham_env_t *env;
        ham_parameter_t parameters[]={
           { HAM_PARAM_CACHESIZE,  (ham_u64_t)1024 },
           { HAM_PARAM_PAGESIZE, (ham_u64_t)1024*4 },
           { HAM_PARAM_KEYSIZE,      (ham_u64_t)20 },
           { 0, 0ull }
        };

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_env_create_ex(env, ".test", m_flags, 0644, &parameters[0]));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createDbWithKeysizeTest(void)
    {
        ham_env_t *env;
        ham_db_t *db;
        ham_parameter_t parameters[]={
           { HAM_PARAM_CACHESIZE,  (ham_u64_t)1024 },
           { HAM_PARAM_PAGESIZE, (ham_u64_t)1024*4 },
           { HAM_PARAM_KEYSIZE,      (ham_u64_t)20 },
           { 0, 0ull }
        };

        ham_parameter_t parameters2[]={
           { HAM_PARAM_KEYSIZE,      (ham_u64_t)64 },
           { 0, 0ull }
        };

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0644));

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_create_db(env, db, 333, 0, parameters));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db, 333, 0, parameters2));
        CPPUNIT_ASSERT_EQUAL((ham_u16_t)64, db_get_keysize(db));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void disableVarkeyTests(void)
    {
        ham_env_t *env;
        ham_db_t *db;
        ham_key_t key;
        ham_record_t rec;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        key.data=(void *)
            "19823918723018702931780293710982730918723091872309187230918";
        key.size=(ham_u16_t)strlen((char *)key.data);
        rec.data=(void *)
            "19823918723018702931780293710982730918723091872309187230918";
        rec.size=(ham_u16_t)strlen((char *)rec.data);


        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0644));

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db, 333, HAM_DISABLE_VAR_KEYLEN, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_KEYSIZE, 
                ham_insert(db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_env_open_db(env, db, 333, HAM_DISABLE_VAR_KEYLEN, 0));
            CPPUNIT_ASSERT_EQUAL(HAM_INV_KEYSIZE, 
                    ham_insert(db, 0, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void multiDbTest(void)
    {
        int i;
        ham_env_t *env;
        ham_db_t *db[10];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
        }

        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void multiDbTest2(void)
    {
        int i;
        ham_env_t *env;
        ham_db_t *db[10];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
        }

        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
        }

        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void multiDbInsertFindTest(void)
    {
        int i;
        const int MAX_DB=5;
        const int MAX_ITEMS=300;
        ham_env_t *env;
        ham_db_t *db[MAX_DB];
        ham_record_t rec;
        ham_key_t key;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));

            for (int j=0; j<MAX_ITEMS; j++) {
                int value=j*(i+1);
                memset(&key, 0, sizeof(key));
                memset(&rec, 0, sizeof(rec));
                key.data=&value;
                key.size=sizeof(value);
                rec.data=&value;
                rec.size=sizeof(value);

                CPPUNIT_ASSERT_EQUAL(0, ham_insert(db[i], 0, &key, &rec, 0));
            }
        }

        for (i=0; i<MAX_DB; i++) {
            for (int j=0; j<MAX_ITEMS; j++) {
                int value=j*(i+1);
                memset(&key, 0, sizeof(key));
                memset(&rec, 0, sizeof(rec));
                key.data=(void *)&value;
                key.size=sizeof(value);

                CPPUNIT_ASSERT_EQUAL(0, ham_find(db[i], 0, &key, &rec, 0));
                CPPUNIT_ASSERT_EQUAL(value, *(int *)key.data);
                CPPUNIT_ASSERT_EQUAL((ham_u16_t)sizeof(value), key.size);
            }
        }

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            for (i=0; i<MAX_DB; i++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
                CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                            (ham_u16_t)i+1, 0, 0));
                for (int j=0; j<MAX_ITEMS; j++) {
                    int value=j*(i+1);
                    memset(&key, 0, sizeof(key));
                    memset(&rec, 0, sizeof(rec));
                    key.data=(void *)&value;
                    key.size=sizeof(value);
    
                    CPPUNIT_ASSERT_EQUAL(0, ham_find(db[i], 0, &key, &rec, 0));
                    CPPUNIT_ASSERT_EQUAL(value, *(int *)key.data);
                    CPPUNIT_ASSERT_EQUAL((ham_u16_t)sizeof(value), key.size);
                }
            }
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void multiDbInsertFindExtendedTest(void)
    {
        int i;
        const int MAX_DB=5;
        const int MAX_ITEMS=300;
        ham_env_t *env;
        ham_db_t *db[MAX_DB];
        ham_record_t rec;
        ham_key_t key;
        char buffer[512];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));

            for (int j=0; j<MAX_ITEMS; j++) {
                int value=j*(i+1);
                memset(&key, 0, sizeof(key));
                memset(&rec, 0, sizeof(rec));
                memset(buffer, (char)value, sizeof(buffer));
                key.data=buffer;
                key.size=sizeof(buffer);
                rec.data=buffer;
                rec.size=sizeof(buffer);
                sprintf(buffer, "%08x%08x", j, i+1);

                CPPUNIT_ASSERT_EQUAL(0, ham_insert(db[i], 0, &key, &rec, 0));
            }
        }

        for (i=0; i<MAX_DB; i++) {
            for (int j=0; j<MAX_ITEMS; j++) {
                int value=j*(i+1);
                memset(&key, 0, sizeof(key));
                memset(&rec, 0, sizeof(rec));
                memset(buffer, (char)value, sizeof(buffer));
                key.data=buffer;
                key.size=sizeof(buffer);
                sprintf(buffer, "%08x%08x", j, i+1);

                CPPUNIT_ASSERT_EQUAL(0, ham_find(db[i], 0, &key, &rec, 0));
                CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(buffer), rec.size);
                CPPUNIT_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
            }
        }

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            for (i=0; i<MAX_DB; i++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
                CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                            (ham_u16_t)i+1, 0, 0));
                for (int j=0; j<MAX_ITEMS; j++) {
                    int value=j*(i+1);
                    memset(&key, 0, sizeof(key));
                    memset(&rec, 0, sizeof(rec));
                    memset(buffer, (char)value, sizeof(buffer));
                    key.data=buffer;
                    key.size=sizeof(buffer);
                    sprintf(buffer, "%08x%08x", j, i+1);
    
                    CPPUNIT_ASSERT_EQUAL(0, ham_find(db[i], 0, &key, &rec, 0));
                    CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(buffer), rec.size);
                    CPPUNIT_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
                }
            }
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void multiDbInsertFindExtendedEraseTest(void)
    {
        int i;
        const int MAX_DB=5;
        const int MAX_ITEMS=300;
        ham_env_t *env;
        ham_db_t *db[MAX_DB];
        ham_record_t rec;
        ham_key_t key;
        char buffer[512];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));

            for (int j=0; j<MAX_ITEMS; j++) {
                int value=j*(i+1);
                memset(&key, 0, sizeof(key));
                memset(&rec, 0, sizeof(rec));
                memset(buffer, (char)value, sizeof(buffer));
                key.data=buffer;
                key.size=sizeof(buffer);
                rec.data=buffer;
                rec.size=sizeof(buffer);
                sprintf(buffer, "%08x%08x", j, i+1);

                CPPUNIT_ASSERT_EQUAL(0, ham_insert(db[i], 0, &key, &rec, 0));
            }
        }

        for (i=0; i<MAX_DB; i++) {
            for (int j=0; j<MAX_ITEMS; j++) {
                int value=j*(i+1);
                memset(&key, 0, sizeof(key));
                memset(&rec, 0, sizeof(rec));
                memset(buffer, (char)value, sizeof(buffer));
                key.data=buffer;
                key.size=sizeof(buffer);
                sprintf(buffer, "%08x%08x", j, i+1);

                CPPUNIT_ASSERT_EQUAL(0, ham_find(db[i], 0, &key, &rec, 0));
                CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(buffer), rec.size);
                CPPUNIT_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
            }
        }

        for (i=0; i<MAX_DB; i++) { 
            for (int j=0; j<MAX_ITEMS; j+=2) { // delete every 2nd entry
                int value=j*(i+1);
                memset(&key, 0, sizeof(key));
                memset(&rec, 0, sizeof(rec));
                memset(buffer, (char)value, sizeof(buffer));
                key.data=buffer;
                key.size=sizeof(buffer);
                sprintf(buffer, "%08x%08x", j, i+1);

                CPPUNIT_ASSERT_EQUAL(0, ham_erase(db[i], 0, &key, 0));
            }
        }

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            for (i=0; i<MAX_DB; i++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
                CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                            (ham_u16_t)i+1, 0, 0));
                for (int j=0; j<MAX_ITEMS; j++) {
                    int value=j*(i+1);
                    memset(&key, 0, sizeof(key));
                    memset(&rec, 0, sizeof(rec));
                    memset(buffer, (char)value, sizeof(buffer));
                    key.data=buffer;
                    key.size=sizeof(buffer);
                    sprintf(buffer, "%08x%08x", j, i+1);
    
                    if (j&1) { // must exist
                        CPPUNIT_ASSERT_EQUAL(0, 
                                ham_find(db[i], 0, &key, &rec, 0));
                        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(buffer), 
                                rec.size);
                        CPPUNIT_ASSERT_EQUAL(0, 
                                memcmp(buffer, rec.data, rec.size));
                    }
                    else { // was deleted
                        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                                ham_find(db[i], 0, &key, &rec, 0));
                    }
                }
            }
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void multiDbInsertCursorTest(void)
    {
        int i;
        const int MAX_DB=5;
        const int MAX_ITEMS=300;
        ham_env_t *env;
        ham_db_t *db[MAX_DB];
        ham_cursor_t *cursor[MAX_DB];
        ham_record_t rec;
        ham_key_t key;
        char buffer[512];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(db[i], 0, 0, &cursor[i]));

            for (int j=0; j<MAX_ITEMS; j++) {
                memset(&key, 0, sizeof(key));
                memset(&rec, 0, sizeof(rec));
                sprintf(buffer, "%08x%08x", j, i+1);
                key.data=buffer;
                key.size=(ham_u16_t)strlen(buffer)+1;
                rec.data=buffer;
                rec.size=(ham_u16_t)strlen(buffer)+1;

                CPPUNIT_ASSERT_EQUAL(0, ham_cursor_insert(cursor[i], 
                            &key, &rec, 0));
            }
        }

        for (i=0; i<MAX_DB; i++) {
            memset(&key, 0, sizeof(key));
            memset(&rec, 0, sizeof(rec));

            CPPUNIT_ASSERT_EQUAL(0, ham_cursor_move(cursor[i], &key, 
                        &rec, HAM_CURSOR_FIRST));
            sprintf(buffer, "%08x%08x", 0, i+1);
            CPPUNIT_ASSERT_EQUAL((ham_size_t)strlen(buffer)+1, rec.size);
            CPPUNIT_ASSERT_EQUAL(0, strcmp(buffer, (char *)rec.data));

            for (int j=1; j<MAX_ITEMS; j++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_cursor_move(cursor[i], &key, 
                        &rec, HAM_CURSOR_NEXT));
                sprintf(buffer, "%08x%08x", j, i+1);
                CPPUNIT_ASSERT_EQUAL((ham_size_t)strlen(buffer)+1, rec.size);
                CPPUNIT_ASSERT_EQUAL(0, strcmp(buffer, (char *)rec.data));
            }
        }

        for (i=0; i<MAX_DB; i++) { 
            for (int j=0; j<MAX_ITEMS; j+=2) { // delete every 2nd entry
                memset(&key, 0, sizeof(key));
                memset(&rec, 0, sizeof(rec));
                sprintf(buffer, "%08x%08x", j, i+1);
                key.data=buffer;
                key.size=(ham_u16_t)strlen(buffer)+1;

                CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(cursor[i], &key, 0));
                CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(cursor[i], 0));
            }
        }

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            for (i=0; i<MAX_DB; i++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor[i]));
                CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
                CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                            (ham_u16_t)i+1, 0, 0));
                CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(db[i], 0, 
                            0, &cursor[i]));
                for (int j=0; j<MAX_ITEMS; j++) {
                    memset(&key, 0, sizeof(key));
                    memset(&rec, 0, sizeof(rec));
                    sprintf(buffer, "%08x%08x", j, i+1);
                    key.data=buffer;
                    key.size=(ham_u16_t)strlen(buffer)+1;
    
                    if (j&1) { // must exist
                        CPPUNIT_ASSERT_EQUAL(0, 
                                ham_cursor_find(cursor[i], &key, 0));
                        CPPUNIT_ASSERT_EQUAL(0, 
                                ham_cursor_move(cursor[i], 0, &rec, 0));
                        CPPUNIT_ASSERT_EQUAL((ham_size_t)strlen(buffer)+1, 
                                rec.size);
                        CPPUNIT_ASSERT_EQUAL(0, 
                                strcmp(buffer, (char *)rec.data));
                    }
                    else { // was deleted
                        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                                ham_cursor_find(cursor[i], &key, 0));
                    }
                }
            }
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void multiDbInsertFindExtendedCloseReopenTest(void)
    {
        int i;
        const int MAX_DB=5;
        const int MAX_ITEMS=300;
        ham_env_t *env;
        ham_db_t *db[MAX_DB];
        ham_record_t rec;
        ham_key_t key;
        char buffer[512];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
        }

        for (i=0; i<MAX_DB; i++) {
            for (int j=0; j<MAX_ITEMS; j++) {
                int value=j*(i+1);
                memset(&key, 0, sizeof(key));
                memset(&rec, 0, sizeof(rec));
                memset(buffer, (char)value, sizeof(buffer));
                key.data=buffer;
                key.size=sizeof(buffer);
                rec.data=buffer;
                rec.size=sizeof(buffer);
                sprintf(buffer, "%08x%08x", j, i+1);

                CPPUNIT_ASSERT_EQUAL(0, ham_insert(db[i], 0, &key, &rec, 0));
            }
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
        }

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, ".test", m_flags));

            for (i=0; i<MAX_DB; i++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                            (ham_u16_t)i+1, 0, 0));
                for (int j=0; j<MAX_ITEMS; j++) {
                    int value=j*(i+1);
                    memset(&key, 0, sizeof(key));
                    memset(&rec, 0, sizeof(rec));
                    memset(buffer, (char)value, sizeof(buffer));
                    key.data=buffer;
                    key.size=sizeof(buffer);
                    sprintf(buffer, "%08x%08x", j, i+1);
    
                    CPPUNIT_ASSERT_EQUAL(0, ham_find(db[i], 0, &key, &rec, 0));
                    CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(buffer), rec.size);
                    CPPUNIT_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
                }
            }

            for (i=0; i<MAX_DB; i++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            }
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void renameOpenDatabases(void)
    {
        int i;
        const int MAX_DB=10;
        ham_env_t *env;
        ham_db_t *db[MAX_DB];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_env_rename_db(env, 
                        (ham_u16_t)i+1, (ham_u16_t)i+1000, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
        }

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            for (i=0; i<MAX_DB; i++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                            (ham_u16_t)i+1000, 0, 0));
            }

            for (i=0; i<MAX_DB; i++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            }
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void renameClosedDatabases(void)
    {
        int i;
        const int MAX_DB=10;
        ham_env_t *env;
        ham_db_t *db[MAX_DB];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_env_rename_db(env, 
                        (ham_u16_t)i+1, (ham_u16_t)i+1000, 0));
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                        (ham_u16_t)i+1000, 0, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void eraseOpenDatabases(void)
    {
        int i;
        const int MAX_DB=1;
        ham_env_t *env;
        ham_db_t *db[MAX_DB];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(HAM_DATABASE_ALREADY_OPEN, 
                            ham_env_erase_db(env, (ham_u16_t)i+1, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
            if (m_flags&HAM_IN_MEMORY_DB)
                CPPUNIT_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND, 
                        ham_env_erase_db(env, (ham_u16_t)i+1, 0));
            else
                CPPUNIT_ASSERT_EQUAL(0, 
                        ham_env_erase_db(env, (ham_u16_t)i+1, 0));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void eraseUnknownDatabases(void)
    {
        int i;
        const int MAX_DB=1;
        ham_env_t *env;
        ham_db_t *db[MAX_DB];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND, 
                            ham_env_erase_db(env, (ham_u16_t)i+1000, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            CPPUNIT_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND, 
                            ham_env_erase_db(env, (ham_u16_t)i+1000, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void eraseMultipleDatabases(void)
    {
        int i, j;
        const int MAX_DB=10;
        const int MAX_ITEMS=300;
        ham_env_t *env;
        ham_db_t *db[MAX_DB];
        ham_record_t rec;
        ham_key_t key;
        char buffer[512];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
            for (j=0; j<MAX_ITEMS; j++) {
                memset(&key, 0, sizeof(key));
                memset(&rec, 0, sizeof(rec));
                sprintf(buffer, "%08x%08x", j, i+1);
                key.data=buffer;
                key.size=sizeof(buffer);
                rec.data=buffer;
                rec.size=sizeof(buffer);

                CPPUNIT_ASSERT_EQUAL(0, ham_insert(db[i], 0, &key, &rec, 0));
            }
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_env_erase_db(env, (ham_u16_t)i+1, 0));
        }

        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND, 
                            ham_env_open_db(env, db[i], (ham_u16_t)i+1, 0, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void endianTestOpenDatabase(void)
    {
        ham_env_t *env;
        ham_db_t *db;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));

        // created by running sample env2
#if HAM_LITTLE_ENDIAN
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, 
                    "data/env-endian-test-open-database-be.hdb", 0));
#else
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, 
                    "data/env-endian-test-open-database-le.hdb", 0));
#endif
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db, 1, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db, 2, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void limitsReachedTest(void)
    {
        int i;
        const int MAX_DB=DB_MAX_INDICES+1;
        ham_env_t *env;
        ham_db_t *db[MAX_DB];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<MAX_DB-1; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
        CPPUNIT_ASSERT_EQUAL(HAM_LIMITS_REACHED, 
                ham_env_create_db(env, db[i], (ham_u16_t)i+1, 0, 0));

        for (i=0; i<MAX_DB-1; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void createEnvOpenDbTest(void)
    {
        ham_env_t *env;
        ham_db_t *db;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(HAM_IO_ERROR, 
                ham_open(db, ".test", m_flags));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
    }
    
    void createFullEnvOpenDbTest(void)
    {
        ham_env_t *env;
        ham_db_t *db;

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db, 111, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_open(db, ".test", m_flags));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
    }

    void createFullEnvOpenSecondDbTest(void)
    {
        ham_env_t *env;
        ham_db_t *db;

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db, 111, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db, 222, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_erase_db(env, 111, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_open(db, ".test", m_flags));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
    }

    void getDatabaseNamesTest(void)
    {
        ham_env_t *env;
        ham_db_t *db1, *db2, *db3;
        ham_u16_t names[5];
        ham_size_t names_size=0;

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db1));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db2));
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db3));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                        ham_env_get_database_names(0, names, &names_size));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                        ham_env_get_database_names(env, 0, &names_size));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                        ham_env_get_database_names(env, names, 0));

        names_size=1;
        CPPUNIT_ASSERT_EQUAL(0,
                        ham_env_get_database_names(env, names, &names_size));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, names_size);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db1, 111, 0, 0));
        names_size=0;
        CPPUNIT_ASSERT_EQUAL(HAM_LIMITS_REACHED,
                        ham_env_get_database_names(env, names, &names_size));

        names_size=1;
        CPPUNIT_ASSERT_EQUAL(0,
                        ham_env_get_database_names(env, names, &names_size));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, names_size);
        CPPUNIT_ASSERT_EQUAL((ham_u16_t)111, names[0]);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db2, 222, 0, 0));
        names_size=1;
        CPPUNIT_ASSERT_EQUAL(HAM_LIMITS_REACHED,
                        ham_env_get_database_names(env, names, &names_size));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db3, 333, 0, 0));
        names_size=5;
        CPPUNIT_ASSERT_EQUAL(0,
                        ham_env_get_database_names(env, names, &names_size));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)3, names_size);
        CPPUNIT_ASSERT_EQUAL((ham_u16_t)111, names[0]);
        CPPUNIT_ASSERT_EQUAL((ham_u16_t)222, names[1]);
        CPPUNIT_ASSERT_EQUAL((ham_u16_t)333, names[2]);

        CPPUNIT_ASSERT_EQUAL(0, ham_close(db2, 0));
        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_env_erase_db(env, 222, 0));
            names_size=5;
            CPPUNIT_ASSERT_EQUAL(0,
                        ham_env_get_database_names(env, names, &names_size));
            CPPUNIT_ASSERT_EQUAL((ham_size_t)2, names_size);
            CPPUNIT_ASSERT_EQUAL((ham_u16_t)111, names[0]);
            CPPUNIT_ASSERT_EQUAL((ham_u16_t)333, names[1]);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_close(db1, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db3, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db1));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db2));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db3));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void maxDatabasesTest(void)
    {
        ham_env_t *env;
        ham_parameter_t ps[]={{HAM_PARAM_MAX_ENV_DATABASES,   0}, {0, 0}};

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        ps[0].value=0;
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_env_create_ex(env, ".test", m_flags, 0664, ps));

        ps[0].value=5;
        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_create_ex(env, ".test", m_flags, 0664, ps));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        if (os_get_pagesize()==1024*16 || m_flags&HAM_IN_MEMORY_DB) {
            ps[0].value=508;
            CPPUNIT_ASSERT_EQUAL(0,
                    ham_env_create_ex(env, ".test", m_flags, 0664, ps));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

            ps[0].value=509;
            CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                    ham_env_create_ex(env, ".test", m_flags, 0664, ps));
        }
        else if (os_get_pagesize()==1024*64) {
            ps[0].value=2044;
            CPPUNIT_ASSERT_EQUAL(0,
                    ham_env_create_ex(env, ".test", m_flags, 0664, ps));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

            ps[0].value=2045;
            CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER,
                    ham_env_create_ex(env, ".test", m_flags, 0664, ps));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void maxDatabasesReopenTest(void)
    {
        ham_env_t *env;
        ham_db_t *db;
        ham_parameter_t ps[]={{HAM_PARAM_MAX_ENV_DATABASES,  50}, {0, 0}};

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));

        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_create_ex(env, ".test", m_flags, 0664, ps));
        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_create_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_open(env, ".test", m_flags));
        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_open_db(env, db, 333, 0, 0));
        CPPUNIT_ASSERT_EQUAL(50u, db_get_max_databases(db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(db));
    }
};

class InMemoryEnvTest : public EnvTest
{
    CPPUNIT_TEST_SUITE(InMemoryEnvTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (openWithKeysizeTest);
    CPPUNIT_TEST      (createWithKeysizeTest);
    CPPUNIT_TEST      (createDbWithKeysizeTest);
    CPPUNIT_TEST      (disableVarkeyTests);
    CPPUNIT_TEST      (envNotEmptyTest);
    CPPUNIT_TEST      (autoCleanupTest);
    CPPUNIT_TEST      (memoryDbTest);
    CPPUNIT_TEST      (multiDbInsertFindTest);
    CPPUNIT_TEST      (multiDbInsertFindExtendedTest);
    CPPUNIT_TEST      (multiDbInsertFindExtendedEraseTest);
    CPPUNIT_TEST      (multiDbInsertCursorTest);
    CPPUNIT_TEST      (multiDbInsertFindExtendedCloseReopenTest);
    CPPUNIT_TEST      (renameOpenDatabases);
    CPPUNIT_TEST      (eraseOpenDatabases);
    CPPUNIT_TEST      (eraseUnknownDatabases);
    CPPUNIT_TEST      (limitsReachedTest);
    CPPUNIT_TEST      (getDatabaseNamesTest);
    CPPUNIT_TEST      (maxDatabasesTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryEnvTest()
    :   EnvTest(HAM_IN_MEMORY_DB)
    {
    }

    void memoryDbTest(void)
    {
        int i;
        ham_env_t *env;
        ham_db_t *db[10];

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));

        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_new(&db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_create_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
        }

        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i], 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EnvTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryEnvTest);
