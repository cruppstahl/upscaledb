/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#ifdef UPS_ENABLE_REMOTE

#include "3rdparty/catch/catch.hpp"

#include <ups/upscaledb_srv.h>
#include <ups/upscaledb_uqi.h>

#include "1errorinducer/errorinducer.h"

#include "utils.h"
#include "os.hpp"

using namespace upscaledb;

#define SERVER_URL "ups://localhost:8989/test.db"

struct RemoteFixture {
  ups_env_t *m_env;
  ups_db_t *m_db;
  ups_srv_t *m_srv;

  RemoteFixture()
    : m_env(0), m_db(0), m_srv(0) {
    ups_srv_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.port = 8989;

    REQUIRE(0 == ups_env_create(&m_env, "test.db",
            UPS_ENABLE_TRANSACTIONS, 0644, 0));

    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 14, UPS_ENABLE_DUPLICATE_KEYS, 0));
    ups_db_close(m_db, 0);

    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 13, UPS_ENABLE_DUPLICATE_KEYS, 0));
    ups_db_close(m_db, 0);

    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 33,
            UPS_RECORD_NUMBER64 | UPS_ENABLE_DUPLICATE_KEYS, 0));
    ups_db_close(m_db, 0);

    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 34, UPS_RECORD_NUMBER32, 0));
    ups_db_close(m_db, 0);

    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 55, 0, 0));
    ups_db_close(m_db, 0);

    REQUIRE(0 == ups_srv_init(&cfg, &m_srv));

    REQUIRE(0 == ups_srv_add_env(m_srv, m_env, "/test.db"));
  }

  ~RemoteFixture() {
    if (m_srv) {
      ups_srv_close(m_srv);
      m_srv = 0;
    }
    ups_env_close(m_env, UPS_AUTO_CLEANUP);
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

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_close(0, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void createCloseOpenCloseTest() {
    ups_env_t *env;

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_close(env, 0));

    REQUIRE(0 ==
      ups_env_open(&env, SERVER_URL, 0, 0));
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
    REQUIRE((uint64_t)579 == params[2].value);
    REQUIRE((uint64_t)UPS_ENABLE_TRANSACTIONS == params[3].value);
    REQUIRE(0644ull == params[4].value);
    REQUIRE(0 == strcmp("test.db", (char *)params[5].value));

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void getDatabaseNamesTest() {
    ups_env_t *env;
    uint16_t names[15];
    uint32_t max_names = 15;

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 ==
        ups_env_get_database_names(env, &names[0], &max_names));

    REQUIRE(14 == names[0]);
    REQUIRE(13 == names[1]);
    REQUIRE(33 == names[2]);
    REQUIRE(34 == names[3]);
    REQUIRE(5u == max_names);

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void envFlushTest() {
    ups_env_t *env;

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 == ups_env_flush(env, 0));

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void renameDbTest() {
    ups_env_t *env;
    uint16_t names[15];
    uint32_t max_names = 15;

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, 0, 0664, 0));

    REQUIRE(0 == ups_env_rename_db(env, 13, 15, 0));
    REQUIRE(0 ==
        ups_env_get_database_names(env, &names[0], &max_names));
    REQUIRE(14 == names[0]);
    REQUIRE(15 == names[1]);
    REQUIRE(33 == names[2]);
    REQUIRE(34 == names[3]);
    REQUIRE(5u == max_names);

    REQUIRE(UPS_DATABASE_NOT_FOUND ==
          ups_env_rename_db(env, 13, 16, 0));
    REQUIRE(0 == ups_env_rename_db(env, 15, 13, 0));
    REQUIRE(0 ==
        ups_env_get_database_names(env, &names[0], &max_names));
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

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ups_env_create_db(env, &db, 22, 0, 0));

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
    REQUIRE(0 ==
        ups_env_create_db(env, &db, 22, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn, ups_db_get_env(db), "name", 0, 0));
    REQUIRE(0 == strcmp("name", ups_txn_get_name(txn)));

    REQUIRE(0 == ups_txn_commit(txn, 0));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void txnBeginAbortTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_txn_t *txn;

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, UPS_ENABLE_TRANSACTIONS, 0664, 0));
    REQUIRE(0 ==
        ups_env_create_db(env, &db, 22, 0, 0));

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
    ups_key_t key = {};
    ups_record_t rec = {};
    ups_record_t rec2 = {};
    uint64_t keycount;

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(UPS_DUPLICATE_KEY == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void insertFindBigTest() {
#define BUFSIZE (1024 * 16 + 10)
    ups_db_t *db;
    ups_env_t *env;
    ups_key_t key = {};
    ups_record_t rec = {};
    ups_record_t rec2 = {};
    uint64_t keycount;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, 0));

    key.data = (void *)"123";
    key.size = 4;
    rec.data = malloc(BUFSIZE);
    rec.size = BUFSIZE;
    memset(rec.data, 0, BUFSIZE);

    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(UPS_DUPLICATE_KEY == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
    free(rec.data);
  }

  void insertFindPartialTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_key_t key = {};
    ups_record_t rec = {};

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 22, 0, 0));

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;
    rec.partial_offset = 0;
    rec.partial_size = 5;

    REQUIRE(UPS_INV_PARAMETER ==
            ups_db_insert(db, 0, &key, &rec, UPS_PARTIAL));

#if 0 /* TODO - partial r/w is disabled with transactions */
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec2.data, "hello\0\0\0\0\0\0\0\0\0"));

    rec.partial_offset=5;
    rec.partial_size=7;
    rec.data=(void *)" chris";
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_PARTIAL | UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp("hello chris", (char *)rec2.data));
#endif

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  template<typename RecnoType>
  void insertRecnoTest(int dbid) {
    ups_db_t *db;
    ups_env_t *env;
    ups_key_t key = {};
    ups_record_t rec = {};

    rec.data = (void *)"hello chris";
    rec.size = 12;

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
    ups_key_t key = {};
    ups_record_t rec = {};
    ups_record_t rec2 = {};
    uint64_t keycount;

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    (void)ups_env_erase_db(env, 33, 0);
    REQUIRE(0 == ups_env_create_db(env, &db, 33, 0, 0));

    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(UPS_DUPLICATE_KEY == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void insertFindEraseUserallocTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_key_t key = {};
    ups_record_t rec = {};
    ups_record_t rec2 = {};
    uint64_t keycount;
    char buf[1024];
    memset(&buf[0], 0, sizeof(buf));

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
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(UPS_DUPLICATE_KEY == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
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
    ups_key_t key = {};
    ups_record_t rec = {};
    ups_record_t rec2 = {};
    uint64_t keycount;
    RecnoType recno;

    rec.data = (void *)"hello chris";
    rec.size = 12;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, dbid, 0, 0));

    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(sizeof(RecnoType) == key.size);
    recno = *(RecnoType *)key.data;
    REQUIRE(1ull == recno);

    REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

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
    ups_key_t key = {};
    ups_cursor_t *cursor;
    ups_record_t rec = {};
    ups_record_t rec2 = {};
    uint64_t keycount;

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;

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

    uint64_t size;
    REQUIRE(0 == ups_cursor_get_record_size(cursor, &size));
    REQUIRE(size == 12);

    REQUIRE(UPS_DUPLICATE_KEY ==
        ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 ==
        ups_cursor_insert(cursor, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorInsertFindPartialTest(void)
  {
    ups_db_t *db;
    ups_env_t *env;
    ups_key_t key = {};
    ups_cursor_t *cursor;
    ups_record_t rec = {};

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;
    rec.partial_offset = 0;
    rec.partial_size = 5;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    ups_env_erase_db(env, 33, 0);
    REQUIRE(0 == ups_env_create_db(env, &db, 33, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
          ups_cursor_insert(cursor, &key, &rec, UPS_PARTIAL));

#if 0 /* TODO - partial r/w is disabled with transactions */
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0));
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec2.data,
          "hello\0\0\0\0\0\0\0\0\0"));

    rec.partial_offset = 5;
    rec.partial_size = 7;
    rec.data = (void *)" chris";
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec,
          UPS_PARTIAL | UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp("hello chris", (char *)rec2.data));
#endif

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  template<typename RecnoType>
  void cursorInsertRecnoTest(int dbid) {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *cursor;
    ups_key_t key = {};
    ups_record_t rec = {};

    rec.data = (void *)"hello chris";
    rec.size = 12;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, dbid, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(sizeof(RecnoType) == key.size);
    REQUIRE(1ull == *(RecnoType *)key.data);

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(sizeof(RecnoType) == key.size);
    REQUIRE(2ull == *(RecnoType *)key.data);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorInsertFindEraseTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_key_t key = {};
    ups_cursor_t *cursor;
    ups_record_t rec = {};
    ups_record_t rec2 = {};
    uint64_t keycount;

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;

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
    REQUIRE(UPS_DUPLICATE_KEY ==
          ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 ==
          ups_cursor_insert(cursor, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
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
    ups_key_t key = {};
    ups_record_t rec = {};
    ups_record_t rec2 = {};
    uint64_t keycount;
    RecnoType recno;

    rec.data = (void *)"hello chris";
    rec.size = 12;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, dbid, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(1ull == keycount);
    REQUIRE(sizeof(RecnoType) == key.size);
    recno = *(RecnoType *)key.data;
    REQUIRE(1ull == recno);

    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    memset(&key, 0, sizeof(key));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(2ull == keycount);
    recno = *(RecnoType *)key.data;
    REQUIRE(2ull == recno);

    memset(&key, 0, sizeof(key));
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
    REQUIRE(UPS_DUPLICATE_KEY ==
          ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(0 ==
          ups_cursor_insert(cursor, &key, &rec, UPS_OVERWRITE));
    memset(&rec2, 0, sizeof(rec2));
    REQUIRE(0 == ups_cursor_find(cursor, &key, &rec2, 0));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));
    REQUIRE(0 == ups_cursor_erase(cursor, 0));
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == ups_db_count(db, 0, 0, &keycount));
    REQUIRE(0ull == keycount);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void insertData(ups_cursor_t *cursor, const char *k, const char *data) {
    ups_key_t key = {};
    ups_record_t rec = {};
    rec.data = (void *)data;
    rec.size = (uint32_t)::strlen(data) + 1;
    key.data = (void *)k;
    key.size = (uint16_t)(k ? ::strlen(k) + 1 : 0);

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

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ups_env_open_db(env, &db, 14, 0, 0));
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_cursor_create(&c, db, txn, 0));

    REQUIRE(UPS_CURSOR_IS_NIL ==
        ups_cursor_get_duplicate_position(c, &position));
    REQUIRE((uint32_t)0 == position);

    insertData(c, "p", "1111111111");
    REQUIRE(0 ==
        ups_cursor_get_duplicate_position(c, &position));
    REQUIRE((uint32_t)0 == position);

    insertData(c, "p", "2222222222");
    REQUIRE(0 ==
        ups_cursor_get_duplicate_position(c, &position));
    REQUIRE((uint32_t)1 == position);

    insertData(c, "p", "3333333333");
    REQUIRE(0 ==
        ups_cursor_get_duplicate_position(c, &position));
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
    ups_key_t key = {};
    ups_cursor_t *cursor;
    ups_record_t rec = {};
    ups_record_t rec2 = {};

    key.data = (void *)"hello world";
    key.size = 12;
    rec.data = (void *)"hello chris";
    rec.size = 12;

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ups_env_open_db(env, &db, 14, 0, 0));
    REQUIRE(0 ==
        ups_cursor_create(&cursor, db, 0, 0));
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
    ups_key_t key = {}, key2 = {};
    key.size = 5;
    ups_record_t rec = {}, rec2 = {};
    rec.size = 5;

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    ups_env_erase_db(env, 14, 0);
    REQUIRE(0 ==
        ups_env_create_db(env, &db, 14, 0, 0));
    REQUIRE(0 ==
        ups_cursor_create(&cursor, db, 0, 0));

    key.data = (void *)"key1";
    rec.data = (void *)"rec1";
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));

    key.data = (void *)"key2";
    rec.data = (void *)"rec2";
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, 0));

    REQUIRE(0 ==
        ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    key.data = (void *)"key1";
    rec.data = (void *)"rec1";
    REQUIRE(0 == ups_cursor_move(cursor, &key2, &rec2, 0));
    REQUIRE(key.size == key2.size);
    REQUIRE(0 == strcmp((char *)key.data, (char *)key2.data));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 ==
        ups_cursor_move(cursor, &key2, &rec2, UPS_CURSOR_NEXT));
    key.data = (void *)"key2";
    rec.data = (void *)"rec2";
    REQUIRE(key.size == key2.size);
    REQUIRE(0 == strcmp((char *)key.data, (char *)key2.data));
    REQUIRE(rec.size == rec2.size);
    REQUIRE(0 == strcmp((char *)rec.data, (char *)rec2.data));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void openTwiceTest() {
    ups_db_t *db1, *db2;
    ups_env_t *env;

    REQUIRE(0 == ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 == ups_env_open_db(env, &db1, 33, 0, 0));
    REQUIRE(UPS_DATABASE_ALREADY_OPEN ==
        ups_env_open_db(env, &db2, 33, 0, 0));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorCreateTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *cursor;

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ups_env_open_db(env, &db, 33, 0, 0));

    REQUIRE(0 ==
        ups_cursor_create(&cursor, db, 0, 0));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void cursorCloneTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_cursor_t *src, *dest;

    REQUIRE(0 ==
        ups_env_create(&env, SERVER_URL, 0, 0664, 0));
    REQUIRE(0 ==
        ups_env_open_db(env, &db, 33, 0, 0));

    REQUIRE(0 ==
        ups_cursor_create(&src, db, 0, 0));
    REQUIRE(0 ==
        ups_cursor_clone(src, &dest));

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

  void autoAbortTransactionTest() {
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
    ErrorInducer::get_instance()->add(ErrorInducer::kServerConnect, 1);

    REQUIRE(UPS_IO_ERROR == ups_env_create(&env,
                SERVER_URL, 0, 0664, &params[0]));
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
    REQUIRE(0 == uqi_select(env, "SUM($key) from database 22", &result));
    REQUIRE(uqi_result_get_record_type(result) == UPS_TYPE_UINT64);
    REQUIRE(*(uint64_t *)uqi_result_get_record_data(result) == sum);
    uqi_result_close(result);

    REQUIRE(0 == uqi_select(env, "count($key) from database 22", &result));
    REQUIRE(uqi_result_get_record_type(result) == UPS_TYPE_UINT64);
    REQUIRE(*(uint64_t *)uqi_result_get_record_data(result) == 50);
    uqi_result_close(result);

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

TEST_CASE("Remote/insertFindPartialTest", "")
{
  RemoteFixture f;
  f.insertFindPartialTest();
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

TEST_CASE("Remote/cursorInsertFindPartialTest", "")
{
  RemoteFixture f;
  f.cursorInsertFindPartialTest();
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

#endif // UPS_ENABLE_REMOTE
