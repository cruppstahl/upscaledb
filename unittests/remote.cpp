/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifdef HAM_ENABLE_REMOTE

#include "../src/config.h"

#include "3rdparty/catch/catch.hpp"

#include "globals.h"
#include "os.hpp"

#include <ham/hamsterdb_srv.h>

#include "../src/env.h"
#include "../src/errorinducer.h"
#include "../src/db_remote.h"
#include "../src/server/hamserver.h"

using namespace hamsterdb;

#define SERVER_URL "ham://localhost:8989/test.db"

struct RemoteFixture {
  ham_env_t *m_env;
  ham_db_t *m_db;
  ham_srv_t *m_srv;

  RemoteFixture()
    : m_env(0), m_db(0), m_srv(0) {
    ham_srv_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.port = 8989;

    REQUIRE(0 == ham_env_create(&m_env, "test.db",
            HAM_ENABLE_TRANSACTIONS, 0644, 0));

    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 14, HAM_ENABLE_DUPLICATE_KEYS, 0));
    ham_db_close(m_db, 0);

    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 13, HAM_ENABLE_DUPLICATE_KEYS, 0));
    ham_db_close(m_db, 0);

    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 33,
            HAM_RECORD_NUMBER | HAM_ENABLE_DUPLICATE_KEYS, 0));
    ham_db_close(m_db, 0);

    REQUIRE(0 == ham_srv_init(&cfg, &m_srv));

    REQUIRE(0 == ham_srv_add_env(m_srv, m_env, "/test.db"));
  }

  ~RemoteFixture() {
    if (m_srv) {
      ham_srv_close(m_srv);
      m_srv = 0;
    }
    ham_env_close(m_env, HAM_AUTO_CLEANUP);
  }

  void invalidUrlTest() {
    ham_env_t *env;

    REQUIRE(HAM_NETWORK_ERROR ==
        ham_env_create(&env, "ham://localhost:77/test.db", 0, 0664, 0));
  }

  void invalidPathTest() {
    ham_env_t *env;

    REQUIRE(HAM_FILE_NOT_FOUND ==
        ham_env_create(&env, "ham://localhost:8989/xxxtest.db", 0, 0, 0));
  }

  void createCloseTest() {
    ham_env_t *env;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_env_close(0, 0));
    REQUIRE(0 == ham_env_close(env, 0));
  }

  void createCloseOpenCloseTest() {
    ham_env_t *env;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ham_env_close(env, 0));

    REQUIRE(0 ==
      ham_env_open(&env, SERVER_URL, 0, 0));
    REQUIRE(0 == ham_env_close(env, 0));
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

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 == ham_env_get_parameters(env, params));

    REQUIRE((unsigned)HAM_DEFAULT_CACHESIZE == params[0].value);
    REQUIRE((ham_u64_t)(1024 * 16) == params[1].value);
    REQUIRE((ham_u64_t)676 == params[2].value);
    REQUIRE((ham_u64_t)(HAM_ENABLE_TRANSACTIONS | HAM_ENABLE_RECOVERY)
           == params[3].value);
    REQUIRE(0644ull == params[4].value);
    REQUIRE(0 == strcmp("test.db", (char *)params[5].value));

    REQUIRE(0 == ham_env_close(env, 0));
  }

  void getDatabaseNamesTest() {
    ham_env_t *env;
    ham_u16_t names[15];
    ham_u32_t max_names = 15;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 ==
        ham_env_get_database_names(env, &names[0], &max_names));

    REQUIRE(14 == names[0]);
    REQUIRE(13 == names[1]);
    REQUIRE(33 == names[2]);
    REQUIRE(3u == max_names);

    REQUIRE(0 == ham_env_close(env, 0));
  }

  void envFlushTest() {
    ham_env_t *env;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 == ham_env_flush(env, 0));

    REQUIRE(0 == ham_env_close(env, 0));
  }

  void renameDbTest() {
    ham_env_t *env;
    ham_u16_t names[15];
    ham_u32_t max_names = 15;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 == ham_env_rename_db(env, 13, 15, 0));
    REQUIRE(0 ==
        ham_env_get_database_names(env, &names[0], &max_names));
    REQUIRE(14 == names[0]);
    REQUIRE(15 == names[1]);
    REQUIRE(33 == names[2]);
    REQUIRE(3u == max_names);

    REQUIRE(HAM_DATABASE_NOT_FOUND ==
          ham_env_rename_db(env, 13, 16, 0));
    REQUIRE(0 == ham_env_rename_db(env, 15, 13, 0));
    REQUIRE(0 ==
        ham_env_get_database_names(env, &names[0], &max_names));
    REQUIRE(14 == names[0]);
    REQUIRE(13 == names[1]);
    REQUIRE(33 == names[2]);
    REQUIRE(3u == max_names);

    REQUIRE(0 == ham_env_close(env, 0));
  }

  void createDbTest() {
    ham_env_t *env;
    ham_db_t *db;

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 22, 0, 0));
    REQUIRE(0x200000000ull == ((RemoteDatabase *)db)->get_remote_handle());

    REQUIRE(0 == ham_db_close(db, 0));
    REQUIRE(0 == ham_env_close(env, 0));
  }

  void createDbExtendedTest() {
    ham_env_t *env;
    ham_db_t *db;
    ham_parameter_t params[] = {
      { HAM_PARAM_KEYSIZE, 5 },
      { 0,0 }
    };

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 22, 0, &params[0]));
    REQUIRE(0x200000000ull == ((RemoteDatabase *)db)->get_remote_handle());

    params[0].value=0;
    REQUIRE(0 == ham_db_get_parameters(db, &params[0]));
    REQUIRE(5ull == params[0].value);

    REQUIRE(0 == ham_db_close(db, 0));
    REQUIRE(0 == ham_env_close(env, 0));
  }

  void openDbTest() {
    ham_env_t *env;
    ham_db_t *db;

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 == ham_env_create_db(env, &db, 22, 0, 0));
    REQUIRE(0x200000000ull == ((RemoteDatabase *)db)->get_remote_handle());
    REQUIRE(0 == ham_db_close(db, 0));

    REQUIRE(0 == ham_env_open_db(env, &db, 22, 0, 0));
    REQUIRE(0x400000001ull == ((RemoteDatabase *)db)->get_remote_handle());
    REQUIRE(0 == ham_db_close(db, 0));

    REQUIRE(0 == ham_env_close(env, 0));
  }

  void eraseDbTest() {
    ham_env_t *env;
    ham_u16_t names[15];
    ham_u32_t max_names = 15;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 ==
        ham_env_get_database_names(env, &names[0], &max_names));
    REQUIRE(14 == names[0]);
    REQUIRE(13 == names[1]);
    REQUIRE(33 == names[2]);
    REQUIRE(3u == max_names);

    REQUIRE(0 == ham_env_erase_db(env, 14, 0));
    REQUIRE(0 ==
        ham_env_get_database_names(env, &names[0], &max_names));
    REQUIRE(13 == names[0]);
    REQUIRE(33 == names[1]);
    REQUIRE(2u == max_names);

    REQUIRE(HAM_DATABASE_NOT_FOUND ==
        ham_env_erase_db(env, 14, 0));

    REQUIRE(0 == ham_env_close(env, 0));
  }

  void getDbParamsTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_parameter_t params[] = {
      { HAM_PARAM_FLAGS, 0 },
      { 0,0 }
    };

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(env, &db, 22, 0, 0));

    REQUIRE(0 == ham_db_get_parameters(db, params));

    REQUIRE((ham_u64_t)(HAM_ENABLE_TRANSACTIONS | HAM_ENABLE_RECOVERY)
           == params[0].value);

    REQUIRE(0 == ham_db_close(db, 0));
    REQUIRE(0 == ham_env_close(env, 0));
  }

  void txnBeginCommitTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_txn_t *txn;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, HAM_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(env, &db, 22, 0, 0));
    REQUIRE(0 == ham_txn_begin(&txn, ham_db_get_env(db), "name", 0, 0));
    REQUIRE(0 == strcmp("name", ham_txn_get_name(txn)));

    REQUIRE(0 == ham_txn_commit(txn, 0));
    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void txnBeginAbortTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_txn_t *txn;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, HAM_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 ==
        ham_env_create_db(env, &db, 22, 0, 0));

    REQUIRE(0 == ham_txn_begin(&txn, ham_db_get_env(db), 0, 0, 0));

    REQUIRE(0 == ham_txn_abort(txn, 0));
    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void checkIntegrityTest() {
    ham_db_t *db;
    ham_env_t *env;

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 22, 0, 0));
    REQUIRE(0 == ham_db_check_integrity(db, 0));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void getKeyCountTest() {
    ham_u64_t keycount;
    ham_db_t *db;
    ham_env_t *env;

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 22, 0, 0));

    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
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

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 22, 0, 0));
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(HAM_DUPLICATE_KEY == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void insertFindBigTest() {
#define BUFSIZE (1024 * 16 + 10)
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_record_t rec2 = {};
    ham_u64_t keycount;

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 22, 0, 0));

    key.data = (void *)"123";
    key.size = 4;
    rec.data = malloc(BUFSIZE);
    rec.size = BUFSIZE;
    memset(rec.data, 0, BUFSIZE);

    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(HAM_DUPLICATE_KEY == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
    free(rec.data);
  }

  void insertFindPartialTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_record_t rec = {};

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 22, 0, 0));

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;
    rec.partial_offset = 0;
    rec.partial_size = 5;

    REQUIRE(HAM_INV_PARAMETER ==
            ham_db_insert(db, 0, &key, &rec, HAM_PARTIAL));

#if 0 /* TODO - partial r/w is disabled with transactions */
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec2.data, "hello\0\0\0\0\0\0\0\0\0"));

    rec.partial_offset=5;
    rec.partial_size=7;
    rec.data=(void *)" chris";
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, HAM_PARTIAL | HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp("hello chris", (char *)rec2.data));
#endif

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void insertRecnoTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_key_t key = {};
    ham_record_t rec = {};

    rec.data = (void *)"hello chris";
    rec.size = 12;

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ham_env_open_db(env, &db, 33, 0, 0));

    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(8 == key.size);
    REQUIRE(1ull == *(ham_u64_t *)key.data);

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(8 == key.size);
    REQUIRE(2ull == *(ham_u64_t *)key.data);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
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

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    (void)ham_env_erase_db(env, 33, 0);
    REQUIRE(0 == ham_env_create_db(env, &db, 33, 0, 0));

    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(HAM_DUPLICATE_KEY == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(0 == ham_db_erase(db, 0, &key, 0));
    REQUIRE(HAM_KEY_NOT_FOUND == ham_db_find(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
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

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    ham_env_erase_db(env, 33, 0);
    REQUIRE(0 == ham_env_create_db(env, &db, 33, 0, 0));
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(HAM_DUPLICATE_KEY == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(0 == ham_db_erase(db, 0, &key, 0));
    REQUIRE(HAM_KEY_NOT_FOUND == ham_db_find(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
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

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_open_db(env, &db, 33, 0, 0));

    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(8 == key.size);
    recno = *(ham_u64_t *)key.data;
    REQUIRE(1ull == recno);

    REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(2ull == keycount);
    recno = *(ham_u64_t *)key.data;
    REQUIRE(2ull == recno);

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(3ull == keycount);
    recno = *(ham_u64_t *)key.data;
    REQUIRE(3ull == recno);

    REQUIRE(0 == ham_db_erase(db, 0, &key, 0));
    REQUIRE(HAM_KEY_NOT_FOUND == ham_db_find(db, 0, &key, &rec, 0));
    REQUIRE(HAM_KEY_NOT_FOUND == ham_db_erase(db, 0, &key, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(2ull == keycount);

    REQUIRE(0 == ham_db_close(db, 0));
    REQUIRE(0 == ham_env_close(env, 0));
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

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    ham_env_erase_db(env, 33, 0);
    REQUIRE(0 ==
        ham_env_create_db(env, &db, 33, 0, 0));
    REQUIRE(0 ==
        ham_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ham_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(HAM_DUPLICATE_KEY ==
        ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 ==
        ham_cursor_insert(cursor, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ham_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
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

    REQUIRE(0 ==
          ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    ham_env_erase_db(env, 33, 0);
    REQUIRE(0 ==
          ham_env_create_db(env, &db, 33, 0, 0));
    REQUIRE(0 ==
          ham_cursor_create(&cursor, db, 0, 0));
    REQUIRE(HAM_INV_PARAMETER ==
          ham_cursor_insert(cursor, &key, &rec, HAM_PARTIAL));

#if 0 /* TODO - partial r/w is disabled with transactions */
    REQUIRE(0 == ham_cursor_find(cursor, &key, 0));
    REQUIRE(0 == ham_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec2.data,
          "hello\0\0\0\0\0\0\0\0\0"));

    rec.partial_offset = 5;
    rec.partial_size = 7;
    rec.data = (void *)" chris";
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec,
          HAM_PARTIAL | HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ham_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp("hello chris", (char *)rec2.data));
#endif

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorInsertRecnoTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_cursor_t *cursor;
    ham_key_t key = {};
    ham_record_t rec = {};

    rec.data = (void *)"hello chris";
    rec.size = 12;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_open_db(env, &db, 33, 0, 0));
    REQUIRE(0 ==
        ham_cursor_create(&cursor, db, 0, 0));

    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(8 == key.size);
    REQUIRE(1ull == *(ham_u64_t *)key.data);

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(8 == key.size);
    REQUIRE(2ull == *(ham_u64_t *)key.data);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
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

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    ham_env_erase_db(env, 33, 0);
    REQUIRE(0 ==
        ham_env_create_db(env, &db, 33, 0, 0));
    REQUIRE(0 ==
        ham_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ham_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(HAM_DUPLICATE_KEY ==
          ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 ==
          ham_cursor_insert(cursor, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ham_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(0 == ham_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ham_cursor_erase(cursor, 0));
    REQUIRE(HAM_KEY_NOT_FOUND == ham_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
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

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_open_db(env, &db, 33, 0, 0));
    REQUIRE(0 ==
        ham_cursor_create(&cursor, db, 0, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(8 == key.size);
    recno = *(ham_u64_t *)key.data;
    REQUIRE(1ull == recno);

    REQUIRE(0 == ham_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(2ull == keycount);
    recno = *(ham_u64_t *)key.data;
    REQUIRE(2ull == recno);

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(3ull == keycount);
    recno = *(ham_u64_t *)key.data;
    REQUIRE(3ull == recno);

    REQUIRE(0 == ham_cursor_erase(cursor, 0));
    REQUIRE(HAM_KEY_NOT_FOUND == ham_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(2ull == keycount);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
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

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    ham_env_erase_db(env, 33, 0);
    REQUIRE(0 ==
        ham_env_create_db(env, &db, 33, 0, 0));
    REQUIRE(0 ==
        ham_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ham_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(HAM_DUPLICATE_KEY ==
          ham_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 ==
          ham_cursor_insert(cursor, &key, &rec, HAM_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ham_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(0 == ham_cursor_erase(cursor, 0));
    REQUIRE(HAM_KEY_NOT_FOUND == ham_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void insertData(ham_cursor_t *cursor, const char *k, const char *data) {
    ham_key_t key = {};
    ham_record_t rec = {};
    rec.data = (void *)data;
    rec.size = (ham_u32_t)::strlen(data)+1;
    key.data = (void *)k;
    key.size = (ham_u16_t)(k ? ::strlen(k)+1 : 0);

    REQUIRE(0 ==
          ham_cursor_insert(cursor, &key, &rec, HAM_DUPLICATE));
  }

  void cursorGetDuplicateCountTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_u32_t count;
    ham_cursor_t *c;
    ham_txn_t *txn;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_open_db(env, &db, 14, 0, 0));
    REQUIRE(0 == ham_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ham_cursor_create(&c, db, txn, 0));

    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_get_duplicate_count(0, &count, 0));
    REQUIRE(HAM_INV_PARAMETER ==
        ham_cursor_get_duplicate_count(c, 0, 0));
    REQUIRE(HAM_CURSOR_IS_NIL ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((ham_u32_t)0 == count);

    insertData(c, 0, "1111111111");
    REQUIRE(0 ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((ham_u32_t)1 == count);

    insertData(c, 0, "2222222222");
    REQUIRE(0 ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((ham_u32_t)2 == count);

    insertData(c, 0, "3333333333");
    REQUIRE(0 ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((ham_u32_t)3 == count);

    REQUIRE(0 == ham_cursor_erase(c, 0));
    REQUIRE(HAM_CURSOR_IS_NIL ==
        ham_cursor_get_duplicate_count(c, &count, 0));

    ham_key_t key;
    memset(&key, 0, sizeof(key));
    REQUIRE(0 ==
        ham_cursor_find(c, &key, 0, 0));
    REQUIRE(0 ==
        ham_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((ham_u32_t)2 == count);

    REQUIRE(0 == ham_cursor_close(c));
    REQUIRE(0 == ham_txn_abort(txn, 0));
    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
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

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_open_db(env, &db, 14, 0, 0));
    REQUIRE(0 ==
        ham_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));

    REQUIRE(0 == ham_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    rec.data = (void *)"hello hamster";
    rec.size = 14;
    REQUIRE(0 ==
        ham_cursor_overwrite(cursor, &rec, 0));
    REQUIRE(0 == ham_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorMoveTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_cursor_t *cursor;
    ham_key_t key = {}, key2 = {};
    key.size = 5;
    ham_record_t rec = {}, rec2 = {};
    rec.size = 5;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    ham_env_erase_db(env, 14, 0);
    REQUIRE(0 ==
        ham_env_create_db(env, &db, 14, 0, 0));
    REQUIRE(0 ==
        ham_cursor_create(&cursor, db, 0, 0));

    key.data = (void *)"key1";
    rec.data = (void *)"rec1";
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));

    key.data = (void *)"key2";
    rec.data = (void *)"rec2";
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, 0));

    REQUIRE(0 ==
        ham_cursor_move(cursor, 0, 0, HAM_CURSOR_FIRST));
    key.data = (void *)"key1";
    rec.data = (void *)"rec1";
    REQUIRE(0 == ham_cursor_move(cursor, &key2, &rec2, 0));
    REQUIRE(key.size == key2.size);
    REQUIRE(0 == strcmp((char *)key.data, (char *)key2.data));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 ==
        ham_cursor_move(cursor, &key2, &rec2, HAM_CURSOR_NEXT));
    key.data = (void *)"key2";
    rec.data = (void *)"rec2";
    REQUIRE(key.size == key2.size);
    REQUIRE(0 == strcmp((char *)key.data, (char *)key2.data));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void openTwiceTest() {
    ham_db_t *db1, *db2;
    ham_env_t *env;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_open_db(env, &db1, 33, 0, 0));
    REQUIRE(HAM_DATABASE_ALREADY_OPEN ==
        ham_env_open_db(env, &db2, 33, 0, 0));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorCreateTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_cursor_t *cursor;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_open_db(env, &db, 33, 0, 0));

    REQUIRE(0 ==
        ham_cursor_create(&cursor, db, 0, 0));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void cursorCloneTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_cursor_t *src, *dest;

    REQUIRE(0 ==
        ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ham_env_open_db(env, &db, 33, 0, 0));

    REQUIRE(0 ==
        ham_cursor_create(&src, db, 0, 0));
    REQUIRE(0 ==
        ham_cursor_clone(src, &dest));

    REQUIRE(0 == ham_cursor_close(src));
    REQUIRE(0 == ham_cursor_close(dest));
    REQUIRE(0 == ham_db_close(db, 0));
    REQUIRE(0 == ham_env_close(env, 0));
  }

  void autoCleanupCursorsTest() {
    ham_env_t *env;
    ham_db_t *db[3];
    ham_cursor_t *c[5];

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    for (int i = 0; i < 3; i++)
      REQUIRE(0 == ham_env_create_db(env, &db[i], i+1, 0, 0));
    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ham_cursor_create(&c[i], db[0], 0, 0));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void autoAbortTransactionTest() {
    ham_env_t *env;
    ham_txn_t *txn;
    ham_db_t *db;

    REQUIRE(0 == ham_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ham_txn_begin(&txn, env, 0, 0, 0));

    REQUIRE(0 == ham_db_close(db, HAM_TXN_AUTO_ABORT));
    REQUIRE(0 == ham_env_close(env, 0));
  }

  void timeoutTest() {
    ham_env_t *env;
    ham_parameter_t params[] = {
      { HAM_PARAM_NETWORK_TIMEOUT_SEC, 2 },
      { 0,0 }
    };

    ErrorInducer *ei = new ErrorInducer();
    ei->add(ErrorInducer::kServerConnect, 1);
    ServerContext *sc = (ServerContext *)m_srv;
    sc->m_inducer = ei;

    REQUIRE(HAM_IO_ERROR == ham_env_create(&env,
                SERVER_URL, 0, 0664, &params[0]));
    delete ei;
  }
};

TEST_CASE("Remote/invalidUrlTest", "")
{
  RemoteFixture f;
  f.invalidUrlTest();
}

TEST_CASE("Remote/invalidPathTest", "")
{
  RemoteFixture f;
  f.invalidPathTest();
}

TEST_CASE("Remote/createCloseTest", "")
{
  RemoteFixture f;
  f.createCloseTest();
}

TEST_CASE("Remote/createCloseOpenCloseTest", "")
{
  RemoteFixture f;
  f.createCloseOpenCloseTest();
}

TEST_CASE("Remote/getEnvParamsTest", "")
{
  RemoteFixture f;
  f.getEnvParamsTest();
}

TEST_CASE("Remote/getDatabaseNamesTest", "")
{
  RemoteFixture f;
  f.getDatabaseNamesTest();
}

TEST_CASE("Remote/envFlushTest", "")
{
  RemoteFixture f;
  f.envFlushTest();
}

TEST_CASE("Remote/renameDbTest", "")
{
  RemoteFixture f;
  f.renameDbTest();
}

TEST_CASE("Remote/createDbTest", "")
{
  RemoteFixture f;
  f.createDbTest();
}

TEST_CASE("Remote/createDbExtendedTest", "")
{
  RemoteFixture f;
  f.createDbExtendedTest();
}

TEST_CASE("Remote/openDbTest", "")
{
  RemoteFixture f;
  f.openDbTest();
}

TEST_CASE("Remote/eraseDbTest", "")
{
  RemoteFixture f;
  f.eraseDbTest();
}

TEST_CASE("Remote/getDbParamsTest", "")
{
  RemoteFixture f;
  f.getDbParamsTest();
}

TEST_CASE("Remote/txnBeginCommitTest", "")
{
  RemoteFixture f;
  f.txnBeginCommitTest();
}

TEST_CASE("Remote/txnBeginAbortTest", "")
{
  RemoteFixture f;
  f.txnBeginAbortTest();
}

TEST_CASE("Remote/checkIntegrityTest", "")
{
  RemoteFixture f;
  f.checkIntegrityTest();
}

TEST_CASE("Remote/getKeyCountTest", "")
{
  RemoteFixture f;
  f.getKeyCountTest();
}

TEST_CASE("Remote/insertFindTest", "")
{
  RemoteFixture f;
  f.insertFindTest();
}

TEST_CASE("Remote/insertFindBigTest", "")
{
  RemoteFixture f;
  f.insertFindBigTest();
}

TEST_CASE("Remote/insertFindPartialTest", "")
{
  RemoteFixture f;
  f.insertFindPartialTest();
}

TEST_CASE("Remote/insertRecnoTest", "")
{
  RemoteFixture f;
  f.insertRecnoTest();
}

TEST_CASE("Remote/insertFindEraseTest", "")
{
  RemoteFixture f;
  f.insertFindEraseTest();
}

TEST_CASE("Remote/insertFindEraseUserallocTest", "")
{
  RemoteFixture f;
  f.insertFindEraseUserallocTest();
}

TEST_CASE("Remote/insertFindEraseRecnoTest", "")
{
  RemoteFixture f;
  f.insertFindEraseRecnoTest();
}

TEST_CASE("Remote/cursorInsertFindTest", "")
{
  RemoteFixture f;
  f.cursorInsertFindTest();
}

TEST_CASE("Remote/cursorInsertFindPartialTest", "")
{
  RemoteFixture f;
  f.cursorInsertFindPartialTest();
}

TEST_CASE("Remote/cursorInsertRecnoTest", "")
{
  RemoteFixture f;
  f.cursorInsertRecnoTest();
}

TEST_CASE("Remote/cursorInsertFindEraseTest", "")
{
  RemoteFixture f;
  f.cursorInsertFindEraseTest();
}

TEST_CASE("Remote/cursorInsertFindEraseUserallocTest", "")
{
  RemoteFixture f;
  f.cursorInsertFindEraseUserallocTest();
}

TEST_CASE("Remote/cursorInsertFindEraseRecnoTest", "")
{
  RemoteFixture f;
  f.cursorInsertFindEraseRecnoTest();
}

TEST_CASE("Remote/cursorGetDuplicateCountTest", "")
{
  RemoteFixture f;
  f.cursorGetDuplicateCountTest();
}

TEST_CASE("Remote/cursorOverwriteTest", "")
{
  RemoteFixture f;
  f.cursorOverwriteTest();
}

TEST_CASE("Remote/cursorMoveTest", "")
{
  RemoteFixture f;
  f.cursorMoveTest();
}

TEST_CASE("Remote/openTwiceTest", "")
{
  RemoteFixture f;
  f.openTwiceTest();
}

TEST_CASE("Remote/cursorCreateTest", "")
{
  RemoteFixture f;
  f.cursorCreateTest();
}

TEST_CASE("Remote/cursorCloneTest", "")
{
  RemoteFixture f;
  f.cursorCloneTest();
}

TEST_CASE("Remote/autoCleanupCursorsTest", "")
{
  RemoteFixture f;
  f.autoCleanupCursorsTest();
}

TEST_CASE("Remote/autoAbortTransactionTest", "")
{
  RemoteFixture f;
  f.autoAbortTransactionTest();
}

TEST_CASE("Remote/timeoutTest", "")
{
  RemoteFixture f;
  f.timeoutTest();
}

#endif // HAM_ENABLE_REMOTE
