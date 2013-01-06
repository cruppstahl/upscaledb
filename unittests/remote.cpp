/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifdef HAM_ENABLE_REMOTE

#include "../src/config.h"

#include <stdexcept>
#include <cstring>
#include <cstdlib>
#include <ham/hamsterdb_int.h>
#include <ham/hamsterdb_srv.h>
#include "../src/env.h"
#include "../src/db.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace hamsterdb;

#define SERVER_URL "http://localhost:8989/test.db"

class RemoteTest : public hamsterDB_fixture
{
  define_super(hamsterDB_fixture);

public:
  RemoteTest()
      : hamsterDB_fixture("RemoteTest") {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(RemoteTest, invalidUrlTest);
    BFC_REGISTER_TEST(RemoteTest, invalidPathTest);
    BFC_REGISTER_TEST(RemoteTest, createCloseTest);
    BFC_REGISTER_TEST(RemoteTest, createCloseOpenCloseTest);
    BFC_REGISTER_TEST(RemoteTest, getEnvParamsTest);
    BFC_REGISTER_TEST(RemoteTest, getDatabaseNamesTest);
    BFC_REGISTER_TEST(RemoteTest, envFlushTest);
    BFC_REGISTER_TEST(RemoteTest, renameDbTest);
    BFC_REGISTER_TEST(RemoteTest, createDbTest);
    BFC_REGISTER_TEST(RemoteTest, createDbExtendedTest);
    BFC_REGISTER_TEST(RemoteTest, openDbTest);
    BFC_REGISTER_TEST(RemoteTest, eraseDbTest);
    BFC_REGISTER_TEST(RemoteTest, getDbParamsTest);
    BFC_REGISTER_TEST(RemoteTest, txnBeginCommitTest);
    BFC_REGISTER_TEST(RemoteTest, txnBeginAbortTest);
    BFC_REGISTER_TEST(RemoteTest, checkIntegrityTest);
    BFC_REGISTER_TEST(RemoteTest, getKeyCountTest);

    BFC_REGISTER_TEST(RemoteTest, insertFindTest);
    BFC_REGISTER_TEST(RemoteTest, insertFindBigTest);
    BFC_REGISTER_TEST(RemoteTest, insertFindPartialTest);
    BFC_REGISTER_TEST(RemoteTest, insertRecnoTest);
    BFC_REGISTER_TEST(RemoteTest, insertFindEraseTest);
    BFC_REGISTER_TEST(RemoteTest, insertFindEraseUserallocTest);
    BFC_REGISTER_TEST(RemoteTest, insertFindEraseRecnoTest);

    BFC_REGISTER_TEST(RemoteTest, cursorInsertFindTest);
    BFC_REGISTER_TEST(RemoteTest, cursorInsertFindPartialTest);
    BFC_REGISTER_TEST(RemoteTest, cursorInsertRecnoTest);
    BFC_REGISTER_TEST(RemoteTest, cursorInsertFindEraseTest);
    BFC_REGISTER_TEST(RemoteTest, cursorInsertFindEraseUserallocTest);
    BFC_REGISTER_TEST(RemoteTest, cursorInsertFindEraseRecnoTest);
    BFC_REGISTER_TEST(RemoteTest, cursorGetDuplicateCountTest);
    BFC_REGISTER_TEST(RemoteTest, cursorOverwriteTest);
    BFC_REGISTER_TEST(RemoteTest, cursorMoveTest);

    BFC_REGISTER_TEST(RemoteTest, openTwiceTest);
    BFC_REGISTER_TEST(RemoteTest, cursorCreateTest);
    BFC_REGISTER_TEST(RemoteTest, cursorCloneTest);
    BFC_REGISTER_TEST(RemoteTest, autoCleanupCursorsTest);
    BFC_REGISTER_TEST(RemoteTest, autoAbortTransactionTest);
    BFC_REGISTER_TEST(RemoteTest, nearFindTest);
  }

protected:
  ham_env_t *m_env;
  ham_db_t *m_db;
  ham_srv_t *m_srv;

  void setup() {
    ham_srv_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.port=8989;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, "test.db",
            HAM_ENABLE_TRANSACTIONS, 0644, 0));

    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 14, HAM_ENABLE_DUPLICATES, 0));
    ham_db_close(m_db, 0);

    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 13, HAM_ENABLE_DUPLICATES, 0));
    ham_db_close(m_db, 0);

    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 33,
            HAM_RECORD_NUMBER|HAM_ENABLE_DUPLICATES, 0));
    ham_db_close(m_db, 0);

    BFC_ASSERT_EQUAL(0,
        ham_srv_init(&cfg, &m_srv));

    BFC_ASSERT_EQUAL(0,
        ham_srv_add_env(m_srv, m_env, "/test.db"));
  }

  void teardown() {
    if (m_srv) {
      ham_srv_close(m_srv);
      m_srv=0;
    }
    ham_env_close(m_env, HAM_AUTO_CLEANUP);
  }

  void invalidUrlTest() {
    ham_env_t *env;

    BFC_ASSERT_EQUAL(HAM_NETWORK_ERROR,
        ham_env_create(&env, "http://localhost:77/test.db", 0, 0664, 0));
  }

  void invalidPathTest() {
    ham_env_t *env;

    BFC_ASSERT_EQUAL(HAM_NETWORK_ERROR,
        ham_env_create(&env, "http://localhost:8989/xxxtest.db", 0, 0, 0));
  }

  void createCloseTest() {
    ham_env_t *env;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_close(0, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void createCloseOpenCloseTest() {
    ham_env_t *env;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

    BFC_ASSERT_EQUAL(0,
      ham_env_open(&env, SERVER_URL, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void getEnvParamsTest() {
    ham_env_t *env;
    ham_parameter_t params[] = {
      { HAM_PARAM_CACHESIZE, 0 },
      { HAM_PARAM_PAGESIZE, 0 },
      { HAM_PARAM_MAX_DATABASES, 0 },
      { HAM_PARAM_FLAGS, 0 },
      { HAM_PARAM_FILEMODE, 0 },
      { HAM_PARAM_FILENAME, 0 },
      { 0,0 }
    };

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));

    BFC_ASSERT_EQUAL(0, ham_env_get_parameters(env, params));

    BFC_ASSERT_EQUAL((unsigned)HAM_DEFAULT_CACHESIZE, params[0].value);
    BFC_ASSERT_EQUAL(1024 * 16u, params[1].value);
    BFC_ASSERT_EQUAL((ham_u64_t)16, params[2].value);
    BFC_ASSERT_EQUAL((ham_u64_t)(HAM_ENABLE_TRANSACTIONS
            | HAM_ENABLE_RECOVERY), params[3].value);
    BFC_ASSERT_EQUAL(0644u, params[4].value);
    BFC_ASSERT_EQUAL(0, strcmp("test.db", (char *)params[5].value));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void getDatabaseNamesTest() {
    ham_env_t *env;
    ham_u16_t names[15];
    ham_size_t max_names = 15;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));

    BFC_ASSERT_EQUAL(0,
        ham_env_get_database_names(env, &names[0], &max_names));

    BFC_ASSERT_EQUAL(14, names[0]);
    BFC_ASSERT_EQUAL(13, names[1]);
    BFC_ASSERT_EQUAL(33, names[2]);
    BFC_ASSERT_EQUAL(3u, max_names);

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void envFlushTest() {
    ham_env_t *env;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));

    BFC_ASSERT_EQUAL(0, ham_env_flush(env, 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void renameDbTest() {
    ham_env_t *env;
    ham_u16_t names[15];
    ham_size_t max_names = 15;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));

    BFC_ASSERT_EQUAL(0, ham_env_rename_db(env, 13, 15, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_get_database_names(env, &names[0], &max_names));
    BFC_ASSERT_EQUAL(14, names[0]);
    BFC_ASSERT_EQUAL(15, names[1]);
    BFC_ASSERT_EQUAL(33, names[2]);
    BFC_ASSERT_EQUAL(3u, max_names);

    BFC_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND,
          ham_env_rename_db(env, 13, 16, 0));
    BFC_ASSERT_EQUAL(0, ham_env_rename_db(env, 15, 13, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_get_database_names(env, &names[0], &max_names));
    BFC_ASSERT_EQUAL(14, names[0]);
    BFC_ASSERT_EQUAL(13, names[1]);
    BFC_ASSERT_EQUAL(33, names[2]);
    BFC_ASSERT_EQUAL(3u, max_names);

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void createDbTest() {
    ham_env_t *env;
    ham_db_t *db;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 22, 0, 0));
    BFC_ASSERT_EQUAL(0x100000000ull,
        ((RemoteDatabase *)db)->get_remote_handle());

    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void createDbExtendedTest() {
    ham_env_t *env;
    ham_db_t *db;
    ham_parameter_t params[] = {
      { HAM_PARAM_KEYSIZE, 5 },
      { 0,0 }
    };

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 22, 0, &params[0]));
    BFC_ASSERT_EQUAL(0x100000000ull,
        ((RemoteDatabase *)db)->get_remote_handle());

    params[0].value=0;
    BFC_ASSERT_EQUAL(0, ham_db_get_parameters(db, &params[0]));
    BFC_ASSERT_EQUAL(5ull, params[0].value);

    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void openDbTest() {
    ham_env_t *env;
    ham_db_t *db;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));

    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 22, 0, 0));
    BFC_ASSERT_EQUAL(0x100000000ull,
        ((RemoteDatabase *)db)->get_remote_handle());
    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));

    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(env, &db, 22, 0, 0));
    BFC_ASSERT_EQUAL(0x200000000ull,
        ((RemoteDatabase *)db)->get_remote_handle());
    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void eraseDbTest() {
    ham_env_t *env;
    ham_u16_t names[15];
    ham_size_t max_names = 15;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));

    BFC_ASSERT_EQUAL(0,
        ham_env_get_database_names(env, &names[0], &max_names));
    BFC_ASSERT_EQUAL(14, names[0]);
    BFC_ASSERT_EQUAL(13, names[1]);
    BFC_ASSERT_EQUAL(33, names[2]);
    BFC_ASSERT_EQUAL(3u, max_names);

    BFC_ASSERT_EQUAL(0, ham_env_erase_db(env, 14, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_get_database_names(env, &names[0], &max_names));
    BFC_ASSERT_EQUAL(13, names[0]);
    BFC_ASSERT_EQUAL(33, names[1]);
    BFC_ASSERT_EQUAL(2u, max_names);

    BFC_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND,
        ham_env_erase_db(env, 14, 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void getDbParamsTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_parameter_t params[] = {
      { HAM_PARAM_FLAGS, 0 },
      { 0,0 }
    };

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 22, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_db_get_parameters(db, params));

    BFC_ASSERT_EQUAL((ham_u64_t)(HAM_ENABLE_TRANSACTIONS
            | HAM_ENABLE_RECOVERY), params[0].value);

    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void txnBeginCommitTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_txn_t *txn;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, HAM_ENABLE_TRANSACTIONS, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 22, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, ham_db_get_env(db), "name", 0, 0));
    BFC_ASSERT_EQUAL(0, strcmp("name", ham_txn_get_name(txn)));

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void txnBeginAbortTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_txn_t *txn;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, HAM_ENABLE_TRANSACTIONS, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 22, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, ham_db_get_env(db), 0, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void checkIntegrityTest() {
    ham_db_t *db;
    ham_env_t *env;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 22, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_check_integrity(db, 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void getKeyCountTest() {
    ham_u64_t keycount;
    ham_db_t *db;
    ham_env_t *env;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 22, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(0ull, keycount);

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void insertFindTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_record_t rec2 = {};
    ham_u64_t keycount;

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 22, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(1ull, keycount);
    BFC_ASSERT_EQUAL(0, ham_db_find(db, 0, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));
    BFC_ASSERT_EQUAL(HAM_DUPLICATE_KEY, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    BFC_ASSERT_EQUAL(0, ham_db_find(db, 0, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void insertFindBigTest() {
#define BUFSIZE (1024 * 16 + 10)
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_record_t rec2 = {};
    ham_u64_t keycount;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 22, 0, 0));

    key.data = (void *)"123";
    key.size = 4;
    rec.data = malloc(BUFSIZE);
    rec.size = BUFSIZE;
    memset(rec.data, 0, BUFSIZE);

    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(1ull, keycount);
    BFC_ASSERT_EQUAL(0, ham_db_find(db, 0, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));
    BFC_ASSERT_EQUAL(HAM_DUPLICATE_KEY, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    BFC_ASSERT_EQUAL(0, ham_db_find(db, 0, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
    free(rec.data);
  }

  void insertFindPartialTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_record_t rec = {};

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 22, 0, 0));

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;
    rec.partial_offset = 0;
    rec.partial_size = 5;

    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
            ham_db_insert(db, 0, &key, &rec, HAM_PARTIAL));

#if 0 /* TODO - partial r/w is disabled with transactions */
    BFC_ASSERT_EQUAL(0, ham_db_find(db, 0, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec2.data,
          "hello\0\0\0\0\0\0\0\0\0"));

    rec.partial_offset=5;
    rec.partial_size=7;
    rec.data=(void *)" chris";
    BFC_ASSERT_EQUAL(0,
                ham_db_insert(db, 0, &key, &rec, HAM_PARTIAL | HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    BFC_ASSERT_EQUAL(0, ham_db_find(db, 0, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp("hello chris", (char *)rec2.data));
#endif

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void insertRecnoTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_record_t rec = {};

    rec.data = (void *)"hello chris";
    rec.size = 12;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(env, &db, 33, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(8, key.size);
    BFC_ASSERT_EQUAL(1ull, *(ham_u64_t *)key.data);

    memset(&key, 0, sizeof(key));
    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(8, key.size);
    BFC_ASSERT_EQUAL(2ull, *(ham_u64_t *)key.data);

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void insertFindEraseTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_record_t rec2 = {};
    ham_u64_t keycount;

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    (void)ham_env_erase_db(env, 33, 0);
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 33, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(1ull, keycount);
    BFC_ASSERT_EQUAL(0, ham_db_find(db, 0, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));
    BFC_ASSERT_EQUAL(HAM_DUPLICATE_KEY, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    BFC_ASSERT_EQUAL(0, ham_db_find(db, 0, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));
    BFC_ASSERT_EQUAL(0, ham_db_erase(db, 0, &key, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, ham_db_find(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(0ull, keycount);

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void insertFindEraseUserallocTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_record_t rec2 = {};
    ham_u64_t keycount;
    char buf[1024];
    memset(&buf[0], 0, sizeof(buf));

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;
    rec2.data = (void *)buf;
    rec2.size = sizeof(buf);
    rec2.flags = HAM_RECORD_USER_ALLOC;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    ham_env_erase_db(env, 33, 0);
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 33, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(1ull, keycount);
    BFC_ASSERT_EQUAL(0, ham_db_find(db, 0, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));
    BFC_ASSERT_EQUAL(HAM_DUPLICATE_KEY, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    BFC_ASSERT_EQUAL(0, ham_db_find(db, 0, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));
    BFC_ASSERT_EQUAL(0, ham_db_erase(db, 0, &key, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, ham_db_find(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(0ull, keycount);

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void insertFindEraseRecnoTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_record_t rec2 = {};
    ham_u64_t keycount;
    ham_u64_t recno;

    rec.data = (void *)"hello chris";
    rec.size = 12;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(env, &db, 33, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(1ull, keycount);
    BFC_ASSERT_EQUAL(8, key.size);
    recno = *(ham_u64_t *)key.data;
    BFC_ASSERT_EQUAL(1ull, recno);

    BFC_ASSERT_EQUAL(0, ham_db_find(db, 0, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));

    memset(&key, 0, sizeof(key));
    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(2ull, keycount);
    recno = *(ham_u64_t *)key.data;
    BFC_ASSERT_EQUAL(2ull, recno);

    memset(&key, 0, sizeof(key));
    BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(3ull, keycount);
    recno = *(ham_u64_t *)key.data;
    BFC_ASSERT_EQUAL(3ull, recno);

    BFC_ASSERT_EQUAL(0, ham_db_erase(db, 0, &key, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, ham_db_find(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, ham_db_erase(db, 0, &key, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(2ull, keycount);

    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void cursorInsertFindTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_cursor_t *cursor;
    ham_record_t rec = {};
    ham_record_t rec2 = {};
    ham_u64_t keycount;

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    ham_env_erase_db(env, 33, 0);
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 33, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_create(&cursor, db, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(1ull, keycount);
    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));
    BFC_ASSERT_EQUAL(HAM_DUPLICATE_KEY,
        ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_insert(cursor, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorInsertFindPartialTest(void)
  {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_cursor_t *cursor;
    ham_record_t rec = {};

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;
    rec.partial_offset = 0;
    rec.partial_size = 5;

    BFC_ASSERT_EQUAL(0,
          ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    ham_env_erase_db(env, 33, 0);
    BFC_ASSERT_EQUAL(0,
          ham_env_create_db(env, &db, 33, 0, 0));
    BFC_ASSERT_EQUAL(0,
          ham_cursor_create(&cursor, db, 0, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
          ham_cursor_insert(cursor, &key, &rec, HAM_PARTIAL));

#if 0 /* TODO - partial r/w is disabled with transactions */
    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec2.data,
          "hello\0\0\0\0\0\0\0\0\0"));

    rec.partial_offset = 5;
    rec.partial_size = 7;
    rec.data = (void *)" chris";
    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec,
          HAM_PARTIAL | HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp("hello chris", (char *)rec2.data));
#endif

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorInsertRecnoTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_cursor_t *cursor;
    ham_key_t key = {};
    ham_record_t rec = {};

    rec.data = (void *)"hello chris";
    rec.size = 12;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(env, &db, 33, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_create(&cursor, db, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(8, key.size);
    BFC_ASSERT_EQUAL(1ull, *(ham_u64_t *)key.data);

    memset(&key, 0, sizeof(key));
    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(8, key.size);
    BFC_ASSERT_EQUAL(2ull, *(ham_u64_t *)key.data);

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorInsertFindEraseTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_cursor_t *cursor;
    ham_record_t rec = {};
    ham_record_t rec2 = {};
    ham_u64_t keycount;

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    ham_env_erase_db(env, 33, 0);
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 33, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_create(&cursor, db, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(1ull, keycount);
    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));
    BFC_ASSERT_EQUAL(HAM_DUPLICATE_KEY,
          ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0,
          ham_cursor_insert(cursor, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));
    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_erase(cursor, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, ham_cursor_find(cursor, &key, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(0ull, keycount);

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorInsertFindEraseRecnoTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_cursor_t *cursor;
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_record_t rec2 = {};
    ham_u64_t keycount;
    ham_u64_t recno;

    rec.data = (void *)"hello chris";
    rec.size = 12;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(env, &db, 33, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_create(&cursor, db, 0, 0));

    memset(&key, 0, sizeof(key));
    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(1ull, keycount);
    BFC_ASSERT_EQUAL(8, key.size);
    recno = *(ham_u64_t *)key.data;
    BFC_ASSERT_EQUAL(1ull, recno);

    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));

    memset(&key, 0, sizeof(key));
    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(2ull, keycount);
    recno = *(ham_u64_t *)key.data;
    BFC_ASSERT_EQUAL(2ull, recno);

    memset(&key, 0, sizeof(key));
    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(3ull, keycount);
    recno = *(ham_u64_t *)key.data;
    BFC_ASSERT_EQUAL(3ull, recno);

    BFC_ASSERT_EQUAL(0, ham_cursor_erase(cursor, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, ham_cursor_find(cursor, &key, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(2ull, keycount);

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorInsertFindEraseUserallocTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_cursor_t *cursor;
    ham_record_t rec = {};
    ham_record_t rec2 = {};
    ham_u64_t keycount;
    char buf[1024];

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;
    rec2.data = (void *)buf;
    rec2.size = sizeof(buf);
    rec2.flags = HAM_RECORD_USER_ALLOC;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    ham_env_erase_db(env, 33, 0);
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 33, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_create(&cursor, db, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(1ull, keycount);
    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));
    BFC_ASSERT_EQUAL(HAM_DUPLICATE_KEY,
          ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0,
          ham_cursor_insert(cursor, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));
    BFC_ASSERT_EQUAL(0, ham_cursor_erase(cursor, 0));
    BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, ham_cursor_find(cursor, &key, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_get_key_count(db, 0, 0, &keycount));
    BFC_ASSERT_EQUAL(0ull, keycount);

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void insertData(ham_cursor_t *cursor, const char *k, const char *data) {
    ham_key_t key = {};
    ham_record_t rec = {};
    rec.data = (void *)data;
    rec.size = (ham_size_t)::strlen(data)+1;
    key.data = (void *)k;
    key.size = (ham_u16_t)(k ? ::strlen(k)+1 : 0);

    BFC_ASSERT_EQUAL(0,
          ham_cursor_insert(cursor, &key, &rec, HAM_DUPLICATE));
  }

  void cursorGetDuplicateCountTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_size_t count;
    ham_cursor_t *c;
    ham_txn_t *txn;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(env, &db, 14, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_create(&c, db, txn, 0));

    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_cursor_get_duplicate_count(0, &count, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_cursor_get_duplicate_count(c, 0, 0));
    BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL,
        ham_cursor_get_duplicate_count(c, &count, 0));
    BFC_ASSERT_EQUAL((ham_size_t)0, count);

    insertData(c, 0, "1111111111");
    BFC_ASSERT_EQUAL(0,
        ham_cursor_get_duplicate_count(c, &count, 0));
    BFC_ASSERT_EQUAL((ham_size_t)1, count);

    insertData(c, 0, "2222222222");
    BFC_ASSERT_EQUAL(0,
        ham_cursor_get_duplicate_count(c, &count, 0));
    BFC_ASSERT_EQUAL((ham_size_t)2, count);

    insertData(c, 0, "3333333333");
    BFC_ASSERT_EQUAL(0,
        ham_cursor_get_duplicate_count(c, &count, 0));
    BFC_ASSERT_EQUAL((ham_size_t)3, count);

    BFC_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
    BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL,
        ham_cursor_get_duplicate_count(c, &count, 0));

    ham_key_t key;
    memset(&key, 0, sizeof(key));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_find(c, &key, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_get_duplicate_count(c, &count, 0));
    BFC_ASSERT_EQUAL((ham_size_t)2, count);

    BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorOverwriteTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_cursor_t *cursor;
    ham_record_t rec = {};
    ham_record_t rec2 = {};

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(env, &db, 14, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_create(&cursor, db, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));

    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));

    rec.data = (void *)"hello hamster";
    rec.size = 14;
    BFC_ASSERT_EQUAL(0,
        ham_cursor_overwrite(cursor, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, &rec2, 0));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorMoveTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_cursor_t *cursor;
    ham_key_t key = {}, key2 = {};
    key.size = 5;
    ham_record_t rec = {}, rec2 = {};
    rec.size = 5;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    ham_env_erase_db(env, 14, 0);
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 14, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_create(&cursor, db, 0, 0));

    key.data = (void *)"key1";
    rec.data = (void *)"rec1";
    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));

    key.data = (void *)"key2";
    rec.data = (void *)"rec2";
    BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));

    BFC_ASSERT_EQUAL(0,
        ham_cursor_move(cursor, 0, 0, HAM_CURSOR_FIRST));
    key.data = (void *)"key1";
    rec.data = (void *)"rec1";
    BFC_ASSERT_EQUAL(0, ham_cursor_move(cursor, &key2, &rec2, 0));
    BFC_ASSERT_EQUAL(key.size, key2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)key.data, (char *)key2.data));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));

    BFC_ASSERT_EQUAL(0,
        ham_cursor_move(cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    key.data = (void *)"key2";
    rec.data = (void *)"rec2";
    BFC_ASSERT_EQUAL(key.size, key2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)key.data, (char *)key2.data));
    BFC_ASSERT_EQUAL(rec.size, rec2.size);
    BFC_ASSERT_EQUAL(0, strcmp((char *)rec.data, (char *)rec2.data));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void openTwiceTest() {
    ham_db_t *db1, *db2;
    ham_env_t *env;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(env, &db1, 33, 0, 0));
    BFC_ASSERT_EQUAL(HAM_DATABASE_ALREADY_OPEN,
        ham_env_open_db(env, &db2, 33, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorCreateTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_cursor_t *cursor;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(env, &db, 33, 0, 0));

    BFC_ASSERT_EQUAL(0,
        ham_cursor_create(&cursor, db, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorCloneTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_cursor_t *src, *dest;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(env, &db, 33, 0, 0));

    BFC_ASSERT_EQUAL(0,
        ham_cursor_create(&src, db, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_clone(src, &dest));

    BFC_ASSERT_EQUAL(0, ham_cursor_close(src));
    BFC_ASSERT_EQUAL(0, ham_cursor_close(dest));
    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void autoCleanupCursorsTest() {
    ham_env_t *env;
    ham_db_t *db[3];
    ham_cursor_t *c[5];

    BFC_ASSERT_EQUAL(0, ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    for (int i = 0; i < 3; i++)
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i], i+1, 0, 0));
    for (int i = 0; i < 5; i++)
      BFC_ASSERT_EQUAL(0, ham_cursor_create(&c[i], db[0], 0, 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void autoAbortTransactionTest() {
    ham_env_t *env;
    ham_txn_t *txn;
    ham_db_t *db;

    BFC_ASSERT_EQUAL(0, ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db, 1, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_db_close(db, HAM_TXN_AUTO_ABORT));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void nearFindTest() {
    unsigned i;
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_record_t rec = {};

    BFC_ASSERT_EQUAL(0, ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db, 13, 0, 0));

    /* empty DB: LT/GT must turn up error */
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_db_find(db, 0, &key, &rec, HAM_FIND_EXACT_MATCH));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_db_find(db, 0, &key, &rec, HAM_FIND_LEQ_MATCH));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_db_find(db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_db_find(db, 0, &key, &rec, HAM_FIND_LT_MATCH));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_db_find(db, 0, &key, &rec, HAM_FIND_GT_MATCH));

    /* insert some values (0, 2, 4) */
    key.data = (void *)&i;
    key.size = sizeof(i);
    for (i = 0; i < 6; i += 2)
      BFC_ASSERT_EQUAL(0, ham_db_insert(db, 0, &key, &rec, 0));

    /* and search for them */
    i = 3;
    key.data = (void *)&i;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_db_find(db, 0, &key, &rec, HAM_FIND_EXACT_MATCH));

    i = 3;
    key.data = (void *)&i;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_db_find(db, 0, &key, &rec, HAM_FIND_LEQ_MATCH));

    i = 3;
    key.data = (void *)&i;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_db_find(db, 0, &key, &rec, HAM_FIND_GEQ_MATCH));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }
};

BFC_REGISTER_FIXTURE(RemoteTest);

#endif // HAM_ENABLE_REMOTE
