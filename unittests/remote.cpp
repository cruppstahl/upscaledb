/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#ifdef UPS_ENABLE_REMOTE

#include "3rdparty/catch/catch.hpp"

#include "ups/upscaledb_srv.h"
#include "ups/upscaledb_uqi.h"
#include "ups/upscaledb_int.h"

#include "1errorinducer/errorinducer.h"

#include "os.hpp"

using namespace upscaledb;

#define SERVER_URL "ups://localhost:8989/test.db"

struct RemoteFixture {
  ups_srv_t *srv;
  ups_env_t *senv;

  RemoteFixture()
    : srv(0) {
    ups_srv_config_t cfg;
    ::memset(&cfg, 0, sizeof(cfg));
    cfg.port = 8989;

    REQUIRE(0 == ups_env_create(&senv, "test.db", UPS_ENABLE_TRANSACTIONS,
                            0644, 0));

    ups_db_t *db;

    REQUIRE(0 == ups_env_create_db(senv, &db, 14,
                            UPS_ENABLE_DUPLICATE_KEYS, 0));
    ups_db_close(db, 0);

    REQUIRE(0 == ups_env_create_db(senv, &db, 13,
                            UPS_ENABLE_DUPLICATE_KEYS, 0));
    ups_db_close(db, 0);

    REQUIRE(0 == ups_env_create_db(senv, &db, 33,
                            UPS_RECORD_NUMBER64 | UPS_ENABLE_DUPLICATE_KEYS, 0));
    ups_db_close(db, 0);

    REQUIRE(0 == ups_env_create_db(senv, &db, 34,
                            UPS_RECORD_NUMBER32, 0));
    ups_db_close(db, 0);

    REQUIRE(0 == ups_env_create_db(senv, &db, 55, 0, 0));
    ups_db_close(db, 0);

    REQUIRE(0 == ups_srv_init(&cfg, &srv));
    REQUIRE(0 == ups_srv_add_env(srv, senv, "/test.db"));
  }

  ~RemoteFixture() {
    if (srv) {
      ups_srv_close(srv);
      srv = 0;
    }
    if (senv) {
      REQUIRE(0 == ups_env_close(senv, UPS_AUTO_CLEANUP));
      senv = 0;
    }
  }

  void invalidUrlTest() {
    ups_env_t *env;

    REQUIRE(UPS_NETWORK_ERROR ==
        ups_env_create(&env, "ups://localhost:77/test.db", 0, 0664, 0));
  }

  void invalidPathTest() {
    ups_env_t *env;

    REQUIRE(UPS_FILE_NOT_FOUND ==
        ups_env_create(&env, "ups://localhost:8989/xxxtest.db", 0, 0, 0));
  }

  void createCloseTest() {
    ups_env_t *env;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_env_close(0, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void createCloseOpenCloseTest() {
    ups_env_t *env;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_close(env, 0));

    REQUIRE(0 == ups_env_open(&env, SERVER_URL, 0, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void getEnvParamsTest() {
    ups_env_t *env;
    ups_parameter_t params[] = {
      { UPS_PARAM_CACHESIZE, 0 },
      { UPS_PARAM_PAGESIZE, 0 },
      { UPS_PARAM_MAX_DATABASES, 0 },
      { UPS_PARAM_FLAGS, 0 },
      { UPS_PARAM_FILEMODE, 0 },
      { UPS_PARAM_FILENAME, 0 },
      { 0,0 }
    };

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_get_parameters(env, params));

    REQUIRE((unsigned)UPS_DEFAULT_CACHE_SIZE == params[0].value);
    REQUIRE((uint64_t)(1024 * 16) == params[1].value);
    REQUIRE((uint64_t)540 == params[2].value);
    REQUIRE((uint64_t)UPS_ENABLE_TRANSACTIONS == params[3].value);
    REQUIRE(0644ull == params[4].value);
    REQUIRE(0 == ::strcmp("test.db", (char *)params[5].value));

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void getDatabaseNamesTest() {
    ups_env_t *env;
    uint16_t names[15];
    uint32_t max_names = 15;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 == ups_env_get_database_names(env, &names[0], &max_names));

    REQUIRE(14 == names[0]);
    REQUIRE(13 == names[1]);
    REQUIRE(33 == names[2]);
    REQUIRE(34 == names[3]);
    REQUIRE(5u == max_names);
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void envFlushTest() {
    ups_env_t *env;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_flush(env, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void renameDbTest() {
    ups_env_t *env;
    uint16_t names[15];
    uint32_t max_names = 15;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 == ups_env_rename_db(env, 13, 15, 0));
    REQUIRE(0 == ups_env_get_database_names(env, &names[0], &max_names));
    REQUIRE(14 == names[0]);
    REQUIRE(15 == names[1]);
    REQUIRE(33 == names[2]);
    REQUIRE(34 == names[3]);
    REQUIRE(5u == max_names);

    REQUIRE(UPS_DATABASE_NOT_FOUND == ups_env_rename_db(env, 13, 16, 0));
    REQUIRE(0 == ups_env_rename_db(env, 15, 13, 0));
    REQUIRE(0 == ups_env_get_database_names(env, &names[0], &max_names));
    REQUIRE(14 == names[0]);
    REQUIRE(13 == names[1]);
    REQUIRE(33 == names[2]);
    REQUIRE(34 == names[3]);
    REQUIRE(5u == max_names);

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void createDbTest() {
    ups_env_t *env;
    ups_db_t *db;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, 0));

    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void createDbExtendedTest() {
    ups_env_t *env;
    ups_db_t *db;
    ups_parameter_t params[] = {
      { UPS_PARAM_KEYSIZE, 5 },
      { 0,0 }
    };

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, &params[0]));

    params[0].value=0;
    REQUIRE(0 == ups_db_get_parameters(db, &params[0]));
    REQUIRE(5ull == params[0].value);

    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void openDbTest() {
    ups_env_t *env;
    ups_db_t *db;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, 0));
    REQUIRE(0 == ups_db_close(db, 0));

    REQUIRE(0 == ups_env_open_db(env, &db, 22, 0, 0));
    REQUIRE(0 == ups_db_close(db, 0));

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void eraseDbTest() {
    ups_env_t *env;
    uint16_t names[15];
    uint32_t max_names = 15;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 == ups_env_get_database_names(env, &names[0], &max_names));
    REQUIRE(14 == names[0]);
    REQUIRE(13 == names[1]);
    REQUIRE(33 == names[2]);
    REQUIRE(34 == names[3]);
    REQUIRE(5u == max_names);

    REQUIRE(0 == ups_env_erase_db(env, 14, 0));
    REQUIRE(0 == ups_env_get_database_names(env, &names[0], &max_names));
    REQUIRE(13 == names[0]);
    REQUIRE(33 == names[1]);
    REQUIRE(34 == names[2]);
    REQUIRE(4u == max_names);

    REQUIRE(UPS_DATABASE_NOT_FOUND == ups_env_erase_db(env, 14, 0));

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void getDbParamsTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_parameter_t params[] = {
        { UPS_PARAM_FLAGS, 0 },
        { 0,0 }
    };

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, 0));

    REQUIRE(0 == ups_db_get_parameters(db, params));

    REQUIRE((uint64_t)UPS_ENABLE_TRANSACTIONS == params[0].value);

    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void txnBeginCommitTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_txn_t *txn;

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, UPS_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn, ups_db_get_env(db), "name", 0, 0));
    REQUIRE(0 == ::strcmp("name", ups_txn_get_name(txn)));

    REQUIRE(0 == ups_txn_commit(txn, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void txnBeginAbortTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_txn_t *txn;

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, UPS_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, 0));

    REQUIRE(0 == ups_txn_begin(&txn, ups_db_get_env(db), 0, 0, 0));

    REQUIRE(0 == ups_txn_abort(txn, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void checkIntegrityTest() {
    ups_db_t *db;
    ups_env_t *env;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, 0));
    REQUIRE(0 == ups_db_check_integrity(db, 0));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void getKeyCountTest() {
    uint64_t keycount;
    ups_db_t *db;
    ups_env_t *env;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, 0));

    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void insertFindTest() {
    ups_db_t *db;
    ups_env_t *env;
    uint64_t keycount;

    ups_key_t key = ups_make_key((void *)"hello world", 12);
    ups_record_t rec = ups_make_record((void *)"hello chris", 12);
    ups_record_t rec2 = {0};

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(UPS_DUPLICATE_KEY == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void insertFindBigTest() {
#define BUFSIZE (1024 * 16 + 10)
    ups_db_t *db;
    ups_env_t *env;
    uint64_t keycount;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, 0));

    std::vector<uint8_t> buffer(BUFSIZE);
    ups_key_t key = ups_make_key((void *)"123", 4);
    ups_record_t rec = ups_make_record(buffer.data(), (uint32_t)buffer.size());
    ups_record_t rec2 = {0};

    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(UPS_DUPLICATE_KEY == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  template<typename RecnoType>
  void insertRecnoTest(int dbid) {
    ups_db_t *db;
    ups_env_t *env;
    ups_key_t key = {0};

    ups_record_t rec = ups_make_record((void *)"hello chris", 12);

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, dbid, 0, 0));

    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(sizeof(RecnoType) == key.size);
    REQUIRE(1ull == *(RecnoType *)key.data);

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(sizeof(RecnoType) == key.size);
    REQUIRE(2ull == *(RecnoType *)key.data);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void insertFindEraseTest() {
    ups_db_t *db;
    ups_env_t *env;
    uint64_t keycount;

    ups_key_t key = ups_make_key((void *)"hello world", 12);
    ups_record_t rec = ups_make_record((void *)"hello chris", 12);
    ups_record_t rec2 = {0};

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    (void)ups_env_erase_db(env, 33, 0);
    REQUIRE(0 == ups_env_create_db(env, &db, 33, 0, 0));

    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(UPS_DUPLICATE_KEY == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void insertFindEraseUserallocTest() {
    ups_db_t *db;
    ups_env_t *env;
    uint64_t keycount;
    std::vector<uint8_t> buf(1024);

    ups_key_t key = ups_make_key((void *)"hello world", 12);
    ups_record_t rec = ups_make_record((void *)"hello chris", 12);
    ups_record_t rec2 = ups_make_record(buf.data(), (uint32_t)buf.size());
    rec2.flags = UPS_RECORD_USER_ALLOC;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    ups_env_erase_db(env, 33, 0);
    REQUIRE(0 == ups_env_create_db(env, &db, 33, 0, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(UPS_DUPLICATE_KEY == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  template<typename RecnoType>
  void insertFindEraseRecnoTest(int dbid) {
    ups_db_t *db;
    ups_env_t *env;
    uint64_t keycount;
    RecnoType recno;

    ups_key_t key = {0};
    ups_record_t rec = ups_make_record((void *)"hello chris", 12);

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, dbid, 0, 0));

    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(sizeof(RecnoType) == key.size);
    recno = *(RecnoType *)key.data;
    REQUIRE(1ull == recno);

    ups_record_t rec2 = {0};
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(2ull == keycount);
    recno = *(RecnoType *)key.data;
    REQUIRE(2ull == recno);

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(3ull == keycount);
    recno = *(RecnoType *)key.data;
    REQUIRE(3ull == recno);

    REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_erase(db, 0, &key, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(2ull == keycount);

    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void cursorInsertFindTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *cursor;
    uint64_t keycount;

    ups_key_t key = ups_make_key((void *)"hello world", 12);
    ups_record_t rec = ups_make_record((void *)"hello chris", 12);
    ups_record_t rec2 = {0};

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    ups_env_erase_db(env, 33, 0);
    REQUIRE(0 == ups_env_create_db(env, &db, 33, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));

    uint32_t size;
    REQUIRE(0 == ups_cursor_get_record_size(cursor, &size));
    REQUIRE(size == 12);

    REQUIRE(UPS_DUPLICATE_KEY ==
        ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  template<typename RecnoType>
  void cursorInsertRecnoTest(int dbid) {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *cursor;
    ups_key_t key = {0};
    ups_record_t rec1 = ups_make_record((void *)"hello1chris", 12);
    ups_record_t rec2 = ups_make_record((void *)"hello2chris", 12);

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, dbid, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec1, 0));
    REQUIRE(sizeof(RecnoType) == key.size);
    REQUIRE(1ull == *(RecnoType *)key.data);

    ::memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec2, 0));
    REQUIRE(sizeof(RecnoType) == key.size);
    REQUIRE(2ull == *(RecnoType *)key.data);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorInsertFindEraseTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *cursor;
    uint64_t keycount;

    ups_key_t key = ups_make_key((void *)"hello world", 12);
    ups_record_t rec = ups_make_record((void *)"hello chris", 12);
    ups_record_t rec2 = {0};

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    ups_env_erase_db(env, 33, 0);
    REQUIRE(0 == ups_env_create_db(env, &db, 33, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(UPS_DUPLICATE_KEY == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, UPS_OVERWRITE));
    ::memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ups_cursor_erase(cursor, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  template<typename RecnoType>
  void cursorInsertFindEraseRecnoTest(int dbid) {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *cursor;
    ups_key_t key = {0};
    ups_record_t rec2 = {0};
    uint64_t keycount;
    RecnoType recno;

    ups_record_t rec = ups_make_record((void *)"hello chris", 12);

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, dbid, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(sizeof(RecnoType) == key.size);
    recno = *(RecnoType *)key.data;
    REQUIRE(1ull == recno);

    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));

    ::memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(2ull == keycount);
    recno = *(RecnoType *)key.data;
    REQUIRE(2ull == recno);

    ::memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(3ull == keycount);
    recno = *(RecnoType *)key.data;
    REQUIRE(3ull == recno);

    REQUIRE(0 == ups_cursor_erase(cursor, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(2ull == keycount);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void approxMatchTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_key_t key = ups_make_key((void *)"k1", 3);
    ups_record_t rec = ups_make_record((void *)"r1", 3);

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 55, 0, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    key.data = (void *)"k2";
    rec.data = (void *)"r2";
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    key.data = (void *)"k3";
    rec.data = (void *)"r3";
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    key.data = (void *)"k2";
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, UPS_FIND_LT_MATCH));
    REQUIRE(0 == ::strcmp((char *)key.data, "k1"));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r1"));

    key.data = (void *)"k2";
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, UPS_FIND_LEQ_MATCH));
    REQUIRE(0 == ::strcmp((char *)key.data, "k2"));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r2"));

    key.data = (void *)"k2";
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, UPS_FIND_GT_MATCH));
    REQUIRE(0 == ::strcmp((char *)key.data, "k3"));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r3"));

    key.data = (void *)"k2";
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, UPS_FIND_GEQ_MATCH));
    REQUIRE(0 == ::strcmp((char *)key.data, "k2"));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r2"));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorApproxMatchTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *cursor;
    ups_key_t key = ups_make_key((void *)"k1", 3);
    ups_record_t rec = ups_make_record((void *)"r1", 3);

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 55, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"k2";
    rec.data = (void *)"r2";
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    key.data = (void *)"k3";
    rec.data = (void *)"r3";
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));

    key.data = (void *)"k2";
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, UPS_FIND_LT_MATCH));
    REQUIRE(0 == ::strcmp((char *)key.data, "k1"));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r1"));

    key.data = (void *)"k2";
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, UPS_FIND_LEQ_MATCH));
    REQUIRE(0 == ::strcmp((char *)key.data, "k2"));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r2"));

    key.data = (void *)"k2";
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, UPS_FIND_GT_MATCH));
    REQUIRE(0 == ::strcmp((char *)key.data, "k3"));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r3"));

    key.data = (void *)"k2";
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec, UPS_FIND_GEQ_MATCH));
    REQUIRE(0 == ::strcmp((char *)key.data, "k2"));
    REQUIRE(0 == ::strcmp((char *)rec.data, "r2"));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorInsertFindEraseUserallocTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_key_t key = {};
    ups_cursor_t *cursor;
    ups_record_t rec = {};
    ups_record_t rec2 = {};
    uint64_t keycount;
    char buf[1024];

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;
    rec2.data = (void *)buf;
    rec2.size = sizeof(buf);
    rec2.flags = UPS_RECORD_USER_ALLOC;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    ups_env_erase_db(env, 33, 0);
    REQUIRE(0 == ups_env_create_db(env, &db, 33, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(UPS_DUPLICATE_KEY == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(0 == ups_cursor_erase(cursor, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void insertData(ups_cursor_t *cursor, const char *k, const char *data) {
    ups_key_t key = ups_make_key((void *)k,
                    (uint16_t)(k ? ::strlen(k) + 1 : 0));
    ups_record_t rec = ups_make_record((void *)data,
                    (uint32_t)(::strlen(data) + 1));

    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, UPS_DUPLICATE));
  }

  void cursorGetDuplicateCountTest() {
    ups_db_t *db;
    ups_env_t *env;
    uint32_t count;
    ups_cursor_t *c;
    ups_txn_t *txn;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 14, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn, 0));

    REQUIRE(UPS_INV_PARAMETER == ups_cursor_get_duplicate_count(0, &count, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_cursor_get_duplicate_count(c, 0, 0));
    REQUIRE(UPS_CURSOR_IS_NIL == ups_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((uint32_t)0 == count);

    insertData(c, 0, "1111111111");
    REQUIRE(0 == ups_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((uint32_t)1 == count);

    insertData(c, 0, "2222222222");
    REQUIRE(0 == ups_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((uint32_t)2 == count);

    insertData(c, 0, "3333333333");
    REQUIRE(0 == ups_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((uint32_t)3 == count);

    REQUIRE(0 == ups_cursor_erase(c, 0));
    REQUIRE(UPS_CURSOR_IS_NIL == ups_cursor_get_duplicate_count(c, &count, 0));

    ups_key_t key = {0};
    REQUIRE(0 == ups_cursor_find(c, &key, 0, 0));
    REQUIRE(0 == ups_cursor_get_duplicate_count(c, &count, 0));
    REQUIRE((uint32_t)2 == count);

    REQUIRE(0 == ups_cursor_close(c));
    REQUIRE(0 == ups_txn_commit(txn, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorGetDuplicatePositionTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *c;
    ups_txn_t *txn;
    uint32_t position = 0;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 14, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn, 0));

    REQUIRE(UPS_CURSOR_IS_NIL ==
        ups_cursor_get_duplicate_position(c, &position));
    REQUIRE((uint32_t)0 == position);

    insertData(c, "p", "1111111111");
    REQUIRE(0 == ups_cursor_get_duplicate_position(c, &position));
    REQUIRE((uint32_t)0 == position);

    insertData(c, "p", "2222222222");
    REQUIRE(0 == ups_cursor_get_duplicate_position(c, &position));
    REQUIRE((uint32_t)1 == position);

    insertData(c, "p", "3333333333");
    REQUIRE(0 == ups_cursor_get_duplicate_position(c, &position));
    REQUIRE((uint32_t)2 == position);

    REQUIRE(0 == ups_cursor_erase(c, 0));
    REQUIRE(UPS_CURSOR_IS_NIL ==
        ups_cursor_get_duplicate_position(c, &position));

    REQUIRE(0 == ups_cursor_close(c));
    REQUIRE(0 == ups_txn_abort(txn, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorOverwriteTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *cursor;
    ups_record_t rec2 = {};

    ups_key_t key = ups_make_key((void *)"hello world", 12);
    ups_record_t rec = ups_make_record((void *)"hello chris", 12);

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 14, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));

    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    rec.data = (void *)"hello upscaledb";
    rec.size = 16;
    REQUIRE(0 == ups_cursor_overwrite(cursor, &rec, 0));
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorMoveTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *cursor;
    ups_key_t key = {}, key2 = {0};
    key.size = 5;
    ups_record_t rec = {}, rec2 = {0};
    rec.size = 5;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    ups_env_erase_db(env, 14, 0);
    REQUIRE(0 == ups_env_create_db(env, &db, 14, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    key.data = (void *)"key1";
    rec.data = (void *)"rec1";
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));

    key.data = (void *)"key2";
    rec.data = (void *)"rec2";
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));

    REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    key.data = (void *)"key1";
    rec.data = (void *)"rec1";
    REQUIRE(0 == ups_cursor_move(cursor, &key2, &rec2, 0));
    REQUIRE(key.size == key2.size);
    REQUIRE(0 == ::strcmp((char *)key.data, (char *)key2.data));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ups_cursor_move(cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    key.data = (void *)"key2";
    rec.data = (void *)"rec2";
    REQUIRE(key.size == key2.size);
    REQUIRE(0 == ::strcmp((char *)key.data, (char *)key2.data));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == ::strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void openTwiceTest() {
    ups_db_t *db1, *db2;
    ups_env_t *env;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db1, 33, 0, 0));
    REQUIRE(UPS_DATABASE_ALREADY_OPEN == ups_env_open_db(env, &db2, 33, 0, 0));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorCreateTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *cursor;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 33, 0, 0));

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorCloneTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *src, *dest;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 33, 0, 0));

    REQUIRE(0 == ups_cursor_create(&src, db, 0, 0));
    REQUIRE(0 == ups_cursor_clone(src, &dest));

    REQUIRE(0 == ups_cursor_close(src));
    REQUIRE(0 == ups_cursor_close(dest));
    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void autoCleanupCursorsTest() {
    ups_env_t *env;
    ups_db_t *db[3];
    ups_cursor_t *c[5];

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    for (int i = 0; i < 3; i++)
      REQUIRE(0 == ups_env_create_db(env, &db[i], i+1, 0, 0));
    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], db[0], 0, 0));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void autoAbortTxnTest() {
    ups_env_t *env;
    ups_txn_t *txn;
    ups_db_t *db;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));

    REQUIRE(0 == ups_db_close(db, UPS_TXN_AUTO_ABORT));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void timeoutTest() {
    ups_env_t *env;
    ups_parameter_t params[] = {
        { UPS_PARAM_NETWORK_TIMEOUT_SEC, 2 },
        { 0,0 }
    };

    ErrorInducer::activate(true);
    ErrorInducer::add(ErrorInducer::kServerConnect, 1);

    REQUIRE(UPS_IO_ERROR == ups_env_create(&env, SERVER_URL, 0,
                            0664, &params[0]));
  }

  void uqiTest() {
    ups_key_t key = {0};
    ups_record_t record = {0};
    uint32_t sum = 0;

    ups_env_t *env;
    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));

    ups_db_t *db;
    ups_parameter_t params[] = {
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32 },
        { 0,0 }
    };
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, &params[0]));

    // insert a few keys
    for (int i = 0; i < 50; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      sum += i;
    }

    uqi_result_t *result;
    uint32_t size;

    REQUIRE(0 == uqi_select(env, "SUM($key) from database 22", &result));
    REQUIRE(uqi_result_get_record_type(result) == UPS_TYPE_UINT64);
    REQUIRE(*(uint64_t *)uqi_result_get_record_data(result, &size) == sum);
    uqi_result_close(result);

    REQUIRE(0 == uqi_select(env, "count($key) from database 22", &result));
    REQUIRE(uqi_result_get_record_type(result) == UPS_TYPE_UINT64);
    REQUIRE(*(uint64_t *)uqi_result_get_record_data(result, &size) == 50);
    uqi_result_close(result);

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void bulkTest() {
    ups_env_t *env;
    ups_db_t *db;
    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

    std::vector<ups_operation_t> ops;

    int i1 = 1;
    ups_key_t key1 = ups_make_key(&i1, sizeof(i1));
    ups_record_t rec1 = ups_make_record(&i1, sizeof(i1));
    ops.push_back({UPS_OP_INSERT, key1, rec1, 0});

    int i2 = 2;
    ups_key_t key2 = ups_make_key(&i2, sizeof(i2));
    ups_record_t rec2 = ups_make_record(&i2, sizeof(i2));
    ops.push_back({UPS_OP_INSERT, key2, rec2, 0});

    int i3 = 3;
    ups_key_t key3 = ups_make_key(&i3, sizeof(i3));
    ups_record_t rec3 = ups_make_record(&i3, sizeof(i3));
    ops.push_back({UPS_OP_INSERT, key3, rec3, 0});

    ups_key_t key4 = ups_make_key(&i2, sizeof(i2));
    ups_record_t rec4 = {0};
    ops.push_back({UPS_OP_FIND, key4, rec4, 0});
    ops.push_back({UPS_OP_ERASE, key4, rec4, 0});

    ups_record_t rec5 = {0};
    ops.push_back({UPS_OP_FIND, key4, rec5, 0});

    REQUIRE(0 == ups_db_bulk_operations(db, 0, ops.data(), ops.size(), 0));
    REQUIRE(0 == ops[0].result);
    REQUIRE(0 == ops[1].result);
    REQUIRE(0 == ops[2].result);
    REQUIRE(0 == ops[3].result);
    REQUIRE(ops[3].key.size == key4.size);
    REQUIRE(0 == ::memcmp(ops[3].key.data, key4.data, key4.size));
    REQUIRE(0 == ops[4].result);
    REQUIRE(UPS_KEY_NOT_FOUND == ops[5].result);

    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void bulkUserAllocTest() {
    ups_env_t *env;
    ups_db_t *db;
    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

    std::vector<ups_operation_t> ops;

    int i1 = 99;
    ups_key_t key1 = ups_make_key(&i1, sizeof(i1));
    ups_record_t rec1 = ups_make_record(&i1, sizeof(i1));
    ops.push_back({UPS_OP_INSERT, key1, rec1, 0});

    ups_key_t key4 = ups_make_key(&i1, sizeof(i1));
    int i4 = 0;
    ups_record_t rec4 = ups_make_record(&i4, sizeof(i4));
    rec4.flags = UPS_RECORD_USER_ALLOC;
    ops.push_back({UPS_OP_FIND, key4, rec4, 0});

    REQUIRE(0 == ups_db_bulk_operations(db, 0, ops.data(), ops.size(), 0));
    REQUIRE(ops[1].key.size == key4.size);
    REQUIRE(i4 == i1);

    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void bulkApproxMatchingTest() {
    ups_env_t *env;
    ups_db_t *db;
    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

    std::vector<ups_operation_t> ops;

    int i1 = 10;
    ups_key_t key1 = ups_make_key(&i1, sizeof(i1));
    ups_record_t rec1 = ups_make_record(&i1, sizeof(i1));
    ops.push_back({UPS_OP_INSERT, key1, rec1, 0});

    int i2 = 20;
    ups_key_t key2 = ups_make_key(&i2, sizeof(i2));
    ups_record_t rec2 = ups_make_record(&i2, sizeof(i2));
    ops.push_back({UPS_OP_INSERT, key2, rec2, 0});

    int i3 = 30;
    ups_key_t key3 = ups_make_key(&i3, sizeof(i3));
    ups_record_t rec3 = ups_make_record(&i3, sizeof(i3));
    ops.push_back({UPS_OP_INSERT, key3, rec3, 0});

    int i4 = i2;
    ups_key_t key4 = ups_make_key(&i4, sizeof(i4));
    key4.flags = UPS_KEY_USER_ALLOC;
    ups_record_t rec4 = {0};
    ops.push_back({UPS_OP_FIND, key4, rec4, UPS_FIND_LT_MATCH});

    int i5 = i2;
    ups_key_t key5 = ups_make_key(&i5, sizeof(i5));
    ups_record_t rec5 = {0};
    ops.push_back({UPS_OP_FIND, key5, rec5, UPS_FIND_GT_MATCH});

    REQUIRE(0 == ups_db_bulk_operations(db, 0, ops.data(), ops.size(), 0));
    REQUIRE(*(int *)ops[3].record.data == i1);
    REQUIRE(i4 == i1);

    REQUIRE(*(int *)ops[4].key.data == i3);

    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void bulkNegativeTests() {
    ups_env_t *env;
    ups_db_t *db;
    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

    std::vector<ups_operation_t> ops;

    REQUIRE(UPS_INV_PARAMETER == ups_db_bulk_operations(0, 0,
                            ops.data(), 0, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_db_bulk_operations(db, 0, 0, 0, 0));

    int i1 = 10;
    ups_key_t key1 = ups_make_key(&i1, sizeof(i1));
    ups_record_t rec1 = ups_make_record(&i1, sizeof(i1));
    ops.push_back({UPS_OP_INSERT, key1, rec1, 0});
    ops.push_back({99, key1, rec1, 0});
    REQUIRE(UPS_INV_PARAMETER == ups_db_bulk_operations(db, 0,
                            ops.data(), 2, 0));

    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, 0));
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

TEST_CASE("Remote/insertRecno64Test", "")
{
  RemoteFixture f;
  f.insertRecnoTest<uint64_t>(33);
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

TEST_CASE("Remote/insertFindEraseRecno64Test", "")
{
  RemoteFixture f;
  f.insertFindEraseRecnoTest<uint64_t>(33);
}

TEST_CASE("Remote/cursorInsertFindTest", "")
{
  RemoteFixture f;
  f.cursorInsertFindTest();
}

TEST_CASE("Remote/cursorInsertRecno64Test", "")
{
  RemoteFixture f;
  f.cursorInsertRecnoTest<uint64_t>(33);
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

TEST_CASE("Remote/cursorInsertFindEraseRecno64Test", "")
{
  RemoteFixture f;
  f.cursorInsertFindEraseRecnoTest<uint64_t>(33);
}

TEST_CASE("Remote/cursorGetDuplicateCountTest", "")
{
  RemoteFixture f;
  f.cursorGetDuplicateCountTest();
}

TEST_CASE("Remote/cursorGetDuplicatePositionTest", "")
{
  RemoteFixture f;
  f.cursorGetDuplicatePositionTest();
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

TEST_CASE("Remote/autoAbortTxnTest", "")
{
  RemoteFixture f;
  f.autoAbortTxnTest();
}

TEST_CASE("Remote/timeoutTest", "")
{
  RemoteFixture f;
  f.timeoutTest();
}

TEST_CASE("Remote/insertRecno32Test", "")
{
  RemoteFixture f;
  f.insertRecnoTest<uint32_t>(34);
}

TEST_CASE("Remote/insertFindEraseRecno32Test", "")
{
  RemoteFixture f;
  f.insertFindEraseRecnoTest<uint32_t>(34);
}

TEST_CASE("Remote/cursorInsertRecno32Test", "")
{
  RemoteFixture f;
  f.cursorInsertRecnoTest<uint32_t>(34);
}

TEST_CASE("Remote/cursorInsertFindEraseRecno32Test", "")
{
  RemoteFixture f;
  f.cursorInsertFindEraseRecnoTest<uint32_t>(34);
}

TEST_CASE("Remote/approxMatchTest", "")
{
  RemoteFixture f;
  f.approxMatchTest();
}

TEST_CASE("Remote/cursorApproxMatchTest", "")
{
  RemoteFixture f;
  f.cursorApproxMatchTest();
}

TEST_CASE("Remote/uqiTest", "")
{
  RemoteFixture f;
  f.uqiTest();
}

TEST_CASE("Remote/bulkTest", "")
{
  RemoteFixture f;
  f.bulkTest();
}

TEST_CASE("Remote/bulkUserAllocTest", "")
{
  RemoteFixture f;
  f.bulkUserAllocTest();
}

TEST_CASE("Remote/bulkApproxMatchingTest", "")
{
  RemoteFixture f;
  f.bulkApproxMatchingTest();
}

TEST_CASE("Remote/bulkNegativeTests", "")
{
  RemoteFixture f;
  f.bulkNegativeTests();
}

#endif // UPS_ENABLE_REMOTE
