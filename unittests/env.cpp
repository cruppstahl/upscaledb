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
    CPPUNIT_TEST      (multiDbTest);
    CPPUNIT_TEST      (multiDbTest2);
    CPPUNIT_TEST      (multiDbInsertFindTest);
    CPPUNIT_TEST      (multiDbInsertFindExtendedTest);
    CPPUNIT_TEST      (multiDbInsertFindExtendedEraseTest);
    CPPUNIT_TEST      (multiDbInsertCursorTest);
    CPPUNIT_TEST      (multiDbInsertFindExtendedCloseTest);
    CPPUNIT_TEST      (renameOpenDatabases);
    CPPUNIT_TEST      (renameClosedDatabases);
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

        CPPUNIT_ASSERT_EQUAL(0, ham_env_create(env, ".test", m_flags, 0664));
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
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
        }

        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));
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
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
        }

        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                        (ham_u16_t)i+1, 0, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));
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
                CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(value), key.size);
            }
        }

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            for (i=0; i<MAX_DB; i++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
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
                    CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(value), key.size);
                }
            }
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));
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
                CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
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
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));
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
                CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
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
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));
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
                key.size=strlen(buffer)+1;
                rec.data=buffer;
                rec.size=strlen(buffer)+1;

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
                key.size=strlen(buffer)+1;

                CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(cursor[i], &key, 0));
                CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(cursor[i], 0));
            }
        }

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            for (i=0; i<MAX_DB; i++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor[i]));
                CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
                CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                            (ham_u16_t)i+1, 0, 0));
                CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(db[i], 0, 
                            0, &cursor[i]));
                for (int j=0; j<MAX_ITEMS; j++) {
                    memset(&key, 0, sizeof(key));
                    memset(&rec, 0, sizeof(rec));
                    sprintf(buffer, "%08x%08x", j, i+1);
                    key.data=buffer;
                    key.size=strlen(buffer)+1;
    
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
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void multiDbInsertFindExtendedCloseTest(void)
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
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
        }

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
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
                CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
            }
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));
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
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
        }

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            for (i=0; i<MAX_DB; i++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                            (ham_u16_t)i+1000, 0, 0));
            }

            for (i=0; i<MAX_DB; i++) {
                CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
            }
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));
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
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_env_rename_db(env, 
                        (ham_u16_t)i+1, (ham_u16_t)i+1000, 0));
        }

        for (i=0; i<MAX_DB; i++) {
            CPPUNIT_ASSERT_EQUAL(0, ham_env_open_db(env, db[i], 
                        (ham_u16_t)i+1000, 0, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

};

class InMemoryEnvTest : public EnvTest
{
    CPPUNIT_TEST_SUITE(InMemoryEnvTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (memoryDbTest);
    CPPUNIT_TEST      (multiDbInsertFindTest);
    CPPUNIT_TEST      (multiDbInsertFindExtendedTest);
    CPPUNIT_TEST      (multiDbInsertFindExtendedEraseTest);
    CPPUNIT_TEST      (multiDbInsertCursorTest);
    CPPUNIT_TEST      (multiDbInsertFindExtendedCloseTest);
    CPPUNIT_TEST      (renameOpenDatabases);
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
            CPPUNIT_ASSERT_EQUAL(0, ham_close(db[i]));
            CPPUNIT_ASSERT_EQUAL(0, ham_delete(db[i]));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EnvTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryEnvTest);
