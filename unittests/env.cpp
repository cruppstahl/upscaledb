/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

#include "../src/env.h"
#include "../src/cache.h"
#include "../src/page.h"
#include "../src/freelist.h"
#include "../src/db.h"
#include "../src/env.h"
#include "../src/os.h"
#include "../src/backend.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace ham;

class EnvTest : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  EnvTest(ham_u32_t flags = 0, const char *name = "EnvTest")
    : hamsterDB_fixture(name), m_flags(flags) {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(EnvTest, createCloseTest);
    BFC_REGISTER_TEST(EnvTest, createCloseOpenCloseTest);
    BFC_REGISTER_TEST(EnvTest, createCloseOpenCloseWithDatabasesTest);
    BFC_REGISTER_TEST(EnvTest, createCloseEmptyOpenCloseWithDatabasesTest);
    BFC_REGISTER_TEST(EnvTest, autoCleanupTest);
    BFC_REGISTER_TEST(EnvTest, autoCleanup2Test);
    BFC_REGISTER_TEST(EnvTest, readOnlyTest);
    BFC_REGISTER_TEST(EnvTest, createPagesizeReopenTest);
    BFC_REGISTER_TEST(EnvTest, openFailCloseTest);
    BFC_REGISTER_TEST(EnvTest, openWithKeysizeTest);
    BFC_REGISTER_TEST(EnvTest, createWithKeysizeTest);
    BFC_REGISTER_TEST(EnvTest, createDbWithKeysizeTest);
    BFC_REGISTER_TEST(EnvTest, createAndOpenMultiDbTest);
    BFC_REGISTER_TEST(EnvTest, disableVarkeyTests);
    BFC_REGISTER_TEST(EnvTest, multiDbTest);
    BFC_REGISTER_TEST(EnvTest, multiDbTest2);
    BFC_REGISTER_TEST(EnvTest, multiDbInsertFindTest);
    BFC_REGISTER_TEST(EnvTest, multiDbInsertFindExtendedTest);
    BFC_REGISTER_TEST(EnvTest, multiDbInsertFindExtendedEraseTest);
    BFC_REGISTER_TEST(EnvTest, multiDbInsertCursorTest);
    BFC_REGISTER_TEST(EnvTest, multiDbInsertFindExtendedCloseReopenTest);
    BFC_REGISTER_TEST(EnvTest, renameOpenDatabases);
    BFC_REGISTER_TEST(EnvTest, renameClosedDatabases);
    BFC_REGISTER_TEST(EnvTest, eraseOpenDatabases);
    BFC_REGISTER_TEST(EnvTest, eraseUnknownDatabases);
    BFC_REGISTER_TEST(EnvTest, eraseMultipleDatabases);
    BFC_REGISTER_TEST(EnvTest, eraseMultipleDatabasesReopenEnv);
    BFC_REGISTER_TEST(EnvTest, endianTestOpenDatabase);
    BFC_REGISTER_TEST(EnvTest, limitsReachedTest);
    BFC_REGISTER_TEST(EnvTest, getDatabaseNamesTest);
    BFC_REGISTER_TEST(EnvTest, maxDatabasesTest);
    BFC_REGISTER_TEST(EnvTest, maxDatabasesReopenTest);
    BFC_REGISTER_TEST(EnvTest, createOpenEmptyTest);
  }

protected:
  ham_u32_t m_flags;

  virtual void setup() {
    __super::setup();

    os::unlink(BFC_OPATH(".test"));
  }

  void createCloseTest() {
    ham_env_t *env;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, ham_env_close(0, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void createCloseOpenCloseTest() {
    ham_env_t *env;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

    if (!(m_flags & HAM_IN_MEMORY)) {
      BFC_ASSERT_EQUAL(0, ham_env_open(&env, BFC_OPATH(".test"), 0, 0));
      BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
    }
  }

  void createCloseOpenCloseWithDatabasesTest() {
    ham_env_t *env;
    ham_db_t *db, *db2;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_create_db(0, &db, 333, 0, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_create_db(env, 0, 333, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db, 333, 0, 0));
    BFC_ASSERT_EQUAL(HAM_DATABASE_ALREADY_EXISTS,
        ham_env_create_db(env, &db2, 333, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));

    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_open_db(0, &db, 333, 0, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_open_db(env, 0, 333, 0, 0));

    if (!(m_flags & HAM_IN_MEMORY)) {
      BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db, 333, 0, 0));
      BFC_ASSERT_EQUAL(HAM_DATABASE_ALREADY_OPEN,
          ham_env_open_db(env, &db, 333, 0, 0));
      BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

      BFC_ASSERT_EQUAL(0, ham_env_open(&env, BFC_OPATH(".test"), 0, 0));

      BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db, 333, 0, 0));
      BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));
    }
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  /*
   * Create and ENV using EXtended parameters, than close it.
   *
   * Open the ENV again and check the parameters before and after
   * creating the first database.
   */
  void createCloseEmptyOpenCloseWithDatabasesTest() {
    ham_env_t *env;
    ham_db_t *db[128], *dbx;
    int i;
    const ham_parameter_t parameters[] = {
       { HAM_PARAM_CACHESIZE, 128 * 1024 },
       { HAM_PARAM_PAGESIZE, 64 * 1024 },
       { HAM_PARAM_MAX_DATABASES, 128 },
       { 0, 0 }
    };
    const ham_parameter_t parameters2[] = {
       { HAM_PARAM_CACHESIZE, 128 * 1024 },
       { 0, 0 }
    };

    ham_parameter_t ps[]={
       { HAM_PARAM_CACHESIZE,0},
       { HAM_PARAM_PAGESIZE, 0},
       { HAM_PARAM_MAX_DATABASES, 0},
       { 0, 0 }
    };

    BFC_ASSERT_EQUAL(0,
      ham_env_create(&env, BFC_OPATH(".test"),
        m_flags, 0664, parameters));
    BFC_ASSERT_EQUAL(0, ham_env_get_parameters(env, ps));
    BFC_ASSERT_EQUAL(128 * 1024u, ps[0].value);
    BFC_ASSERT_EQUAL(64 * 1024u, ps[1].value);
    BFC_ASSERT_EQUAL(128u /* 2029 */, ps[2].value);

    /* close and re-open the ENV */
    if (!(m_flags & HAM_IN_MEMORY)) {
      BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

      BFC_ASSERT_EQUAL(0,
        ham_env_open(&env, BFC_OPATH(".test"), m_flags, parameters2));
    }

    BFC_ASSERT_EQUAL(0, ham_env_get_parameters(env, ps));
    BFC_ASSERT_EQUAL(128 * 1024u, ps[0].value);
    BFC_ASSERT_EQUAL(1024 * 64u, ps[1].value);
    BFC_ASSERT_EQUAL(128u, ps[2].value);

    /* now create 128 DBs; we said we would, anyway, when creating the
     * ENV ! */
    for (i = 0; i < 128; i++) {
      int j;

      BFC_ASSERT_EQUAL_I(0,
          ham_env_create_db(env, &db[i], i + 100, 0, 0), i);
      BFC_ASSERT_EQUAL_I(HAM_DATABASE_ALREADY_EXISTS,
          ham_env_create_db(env, &dbx, i + 100, 0, 0), i);
      BFC_ASSERT_EQUAL_I(0, ham_db_close(db[i], 0), i);
      BFC_ASSERT_EQUAL_I(0,
          ham_env_open_db(env, &db[i], i + 100, 0, 0), i);

      for (j = 0; ps[j].name; j++)
        ps[j].value = 0;
    }

    for (i = 0; i < 128; i++)
      BFC_ASSERT_EQUAL_I(0, ham_db_close(db[i], 0), i);

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void autoCleanupTest(void)
  {
    ham_env_t *env;
    ham_db_t *db[3];
    ham_cursor_t *c[5];

    BFC_ASSERT_EQUAL(0,
      ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));
    for (int i = 0; i < 3; i++)
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i], i+1, 0, 0));
    for (int i = 0; i < 5; i++)
      BFC_ASSERT_EQUAL(0, ham_cursor_create(db[0], 0, 0, &c[i]));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void autoCleanup2Test() {
    ham_env_t *env;
    ham_db_t *db;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));
    BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db, 1, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void readOnlyTest() {
    ham_db_t *db, *db2;
    ham_env_t *env;
    ham_key_t key;
    ham_record_t rec;
    ham_cursor_t *cursor;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    BFC_ASSERT_EQUAL(0, ham_env_create(&env, BFC_OPATH(".test"), 0, 0664, 0));
    BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db, 333, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

    BFC_ASSERT_EQUAL(0, ham_env_open(&env, BFC_OPATH(".test"), HAM_READ_ONLY, 0));
    BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db, 333, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &cursor));
    BFC_ASSERT_EQUAL(HAM_DATABASE_ALREADY_OPEN,
        ham_env_open_db(env, &db2, 333, 0, 0));
    BFC_ASSERT_EQUAL(HAM_WRITE_PROTECTED,
        ham_env_create_db(env, &db2, 444, 0, 0));

    BFC_ASSERT_EQUAL(HAM_WRITE_PROTECTED,
        ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(HAM_WRITE_PROTECTED,
        ham_db_erase(db, 0, &key, 0));
    BFC_ASSERT_EQUAL(HAM_WRITE_PROTECTED,
        ham_cursor_overwrite(cursor, &rec, 0));
    BFC_ASSERT_EQUAL(HAM_WRITE_PROTECTED,
        ham_cursor_insert(cursor, &key, &rec, 0));
    BFC_ASSERT_EQUAL(HAM_WRITE_PROTECTED,
        ham_cursor_erase(cursor, 0));

    BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void createPagesizeReopenTest() {
    ham_env_t *env;
    ham_parameter_t ps[] = { { HAM_PARAM_PAGESIZE, 1024 * 128 }, { 0, 0 } };

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0644, &ps[0]));
    if (!(m_flags & HAM_IN_MEMORY)) {
      BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
      BFC_ASSERT_EQUAL(0,
          ham_env_open(&env, BFC_OPATH(".test"), m_flags, 0));
    }
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void openFailCloseTest() {
    ham_env_t *env;

    BFC_ASSERT_EQUAL(HAM_FILE_NOT_FOUND,
        ham_env_open(&env, "xxxxxx...", 0, 0));
    BFC_ASSERT_EQUAL((ham_env_t *)0, env);
  }

  void openWithKeysizeTest() {
    ham_env_t *env;

    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_open(0, BFC_OPATH(".test"), m_flags, 0));
    BFC_ASSERT_EQUAL(HAM_FILE_NOT_FOUND,
        ham_env_open(&env, BFC_OPATH(".test"), m_flags, 0));
  }

  void createWithKeysizeTest() {
    ham_env_t *env;
    ham_parameter_t parameters[] = {
       { HAM_PARAM_PAGESIZE, (ham_u64_t)1024 * 4 },
       { HAM_PARAM_KEYSIZE,    (ham_u64_t)20 },
       { HAM_PARAM_CACHESIZE,  (ham_u64_t)1024 * 128 },
       { 0, 0ull }
    };

    // in-memory db does not allow the cachesize parameter
    if (m_flags & HAM_IN_MEMORY) {
      parameters[2].name = 0;
      parameters[2].value = 0;
    }

    // it's okay to spec keysize for the ENV: it's used as the
    // default keysize for all DBs within the ENV
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags,
            0644, &parameters[0]));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void createDbWithKeysizeTest() {
    ham_env_t *env;
    ham_db_t *db;
    ham_parameter_t parameters2[] = {
       { HAM_PARAM_KEYSIZE, (ham_u64_t)64 },
       { 0, 0ull }
    };

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0644, 0));

    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 333, 0, parameters2));
    BFC_ASSERT_EQUAL((ham_u16_t)64, db_get_keysize((Database *)db));
    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  // check to make sure both create and open_ex support accessing more
  // than DB_MAX_INDICES DBs in one env:
  void createAndOpenMultiDbTest() {
#define MAX 256
    ham_env_t *env;
    ham_db_t *db[MAX];
    int i;
    ham_key_t key;
    ham_record_t rec;
    ham_parameter_t parameters[] = {
       { HAM_PARAM_KEYSIZE, 20 },
       { 0, 0 }
    };

    ham_parameter_t parameters2[] = {
       { HAM_PARAM_CACHESIZE, 1024 * 128 },
       { HAM_PARAM_PAGESIZE, 1024 * 4 },
       { HAM_PARAM_MAX_DATABASES, MAX },
       { 0, 0 }
    };

    ham_parameter_t parameters3[] = {
       { HAM_PARAM_CACHESIZE, 1024 * 128 },
       { 0, 0 }
    };

    if (m_flags & HAM_IN_MEMORY) {
      BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_create(&env, BFC_OPATH(".test"),
            m_flags, 0644, parameters2));
      parameters2[1].value = 0; // pagesize := 0
    }
    else {
      BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_create(&env, BFC_OPATH(".test"),
          m_flags | HAM_CACHE_UNLIMITED, 0644, parameters2));
      BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_create(&env, BFC_OPATH(".test"),
          m_flags, 0644, parameters2)); // pagesize too small for DB#
      parameters2[1].value = 65536; // pagesize := 64K
    }

    if (m_flags & HAM_IN_MEMORY)
      parameters2[0].value = 0; // cachesize := 0

    BFC_ASSERT_EQUAL(0,
      ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0644, parameters2));

    // create DBs
    for (i = 0; i < MAX; i++) {
      BFC_ASSERT_EQUAL(0,
          ham_env_create_db(env, &db[i], i + 1, 0, parameters));
      memset(&key, 0, sizeof(key));
      memset(&rec, 0, sizeof(rec));
      key.data = &i;
      key.size = sizeof(i);
      rec.data = &i;
      rec.size = sizeof(i);
      BFC_ASSERT_EQUAL(0, ham_db_insert(db[i], 0, &key, &rec, 0));
      if (!(m_flags & HAM_IN_MEMORY))
        BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
    }

    if (!(m_flags & HAM_IN_MEMORY)) {
      BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

      // open DBs
      // pagesize param not allowed
      BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_open(&env, BFC_OPATH(".test"), m_flags, parameters2));
      BFC_ASSERT_EQUAL(0,
        ham_env_open(&env, BFC_OPATH(".test"), m_flags, parameters3));
      // keysize param not allowed
      BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_open_db(env, &db[0], 1, 0, parameters));
    }

    for (i = 0; i < MAX; i++) {
      if (!(m_flags & HAM_IN_MEMORY)) {
        BFC_ASSERT_EQUAL(0,
            ham_env_open_db(env, &db[i], i + 1, 0, 0));
      }
      memset(&key, 0, sizeof(key));
      memset(&rec, 0, sizeof(rec));
      key.data = &i;
      key.size = sizeof(i);
      BFC_ASSERT_EQUAL(0, ham_db_find(db[i], 0, &key, &rec, 0));
      BFC_ASSERT_EQUAL(key.data, &i);
      BFC_ASSERT_EQUAL((rec.data != 0), !0);
      BFC_ASSERT_EQUAL((rec.data != 0
          ? ((int *)rec.data)[0] == i
          : !0), !0);
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
    }

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void disableVarkeyTests() {
    ham_env_t *env;
    ham_db_t *db;
    ham_key_t key;
    ham_record_t rec;

    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));
    key.data = (void *)
      "19823918723018702931780293710982730918723091872309187230918";
    key.size = (ham_u16_t)strlen((char *)key.data);
    rec.data = (void *)
      "19823918723018702931780293710982730918723091872309187230918";
    rec.size = (ham_u16_t)strlen((char *)rec.data);

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 333, HAM_DISABLE_VAR_KEYLEN, 0));
    BFC_ASSERT_EQUAL(HAM_INV_KEYSIZE,
        ham_db_insert(db, 0, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));

    if (!(m_flags & HAM_IN_MEMORY)) {
      BFC_ASSERT_EQUAL(0,
          ham_env_open_db(env, &db, 333, HAM_DISABLE_VAR_KEYLEN, 0));
      BFC_ASSERT_EQUAL(HAM_INV_KEYSIZE,
          ham_db_insert(db, 0, &key, &rec, 0));
      BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));
    }

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void multiDbTest() {
    int i;
    ham_env_t *env;
    ham_db_t *db[10];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < 10; i++) {
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
      BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
    }

    for (i = 0; i < 10; i++) {
      BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
    }

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void multiDbTest2() {
    int i;
    ham_env_t *env;
    ham_db_t *db[10];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < 10; i++)
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));

    for (i = 0; i < 10; i++)
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));

    if (!(m_flags & HAM_IN_MEMORY)) {
      for (i = 0; i < 10; i++) {
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db[i],
              (ham_u16_t)i + 1, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
      }
    }

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void multiDbInsertFindTest() {
    int i;
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ham_env_t *env;
    ham_db_t *db[MAX_DB];
    ham_record_t rec;
    ham_key_t key;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));

      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        key.data = &value;
        key.size = sizeof(value);
        rec.data = &value;
        rec.size = sizeof(value);

        BFC_ASSERT_EQUAL(0, ham_db_insert(db[i], 0, &key, &rec, 0));
      }
    }

    for (i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        key.data = (void *)&value;
        key.size = sizeof(value);

        BFC_ASSERT_EQUAL(0, ham_db_find(db[i], 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(value, *(int *)key.data);
        BFC_ASSERT_EQUAL((ham_u16_t)sizeof(value), key.size);
      }
    }

    if (!(m_flags & HAM_IN_MEMORY)) {
      for (i = 0; i < MAX_DB; i++) {
        BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db[i],
              (ham_u16_t)i + 1, 0, 0));
        for (int j = 0; j < MAX_ITEMS; j++) {
          int value = j * (i + 1);
          memset(&key, 0, sizeof(key));
          memset(&rec, 0, sizeof(rec));
          key.data = (void *)&value;
          key.size = sizeof(value);

          BFC_ASSERT_EQUAL(0, ham_db_find(db[i], 0, &key, &rec, 0));
          BFC_ASSERT_EQUAL(value, *(int *)key.data);
          BFC_ASSERT_EQUAL((ham_u16_t)sizeof(value), key.size);
        }
      }
    }

    for (i = 0; i < MAX_DB; i++)
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void multiDbInsertFindExtendedTest() {
    int i;
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ham_env_t *env;
    ham_db_t *db[MAX_DB];
    ham_record_t rec;
    ham_key_t key;
    char buffer[512];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));

      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        memset(buffer, (char)value, sizeof(buffer));
        key.data = buffer;
        key.size = sizeof(buffer);
        rec.data = buffer;
        rec.size = sizeof(buffer);
        sprintf(buffer, "%08x%08x", j, i + 1);

        BFC_ASSERT_EQUAL(0, ham_db_insert(db[i], 0, &key, &rec, 0));
      }
    }

    for (i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        memset(buffer, (char)value, sizeof(buffer));
        key.data = buffer;
        key.size = sizeof(buffer);
        sprintf(buffer, "%08x%08x", j, i+1);

        BFC_ASSERT_EQUAL(0, ham_db_find(db[i], 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_size_t)sizeof(buffer), rec.size);
        BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
      }
    }

    if (!(m_flags & HAM_IN_MEMORY)) {
      for (i = 0; i < MAX_DB; i++) {
        BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db[i],
              (ham_u16_t)i + 1, 0, 0));
        for (int j = 0; j < MAX_ITEMS; j++) {
          int value = j * (i + 1);
          memset(&key, 0, sizeof(key));
          memset(&rec, 0, sizeof(rec));
          memset(buffer, (char)value, sizeof(buffer));
          key.data = buffer;
          key.size = sizeof(buffer);
          sprintf(buffer, "%08x%08x", j, i+1);

          BFC_ASSERT_EQUAL(0, ham_db_find(db[i], 0, &key, &rec, 0));
          BFC_ASSERT_EQUAL((ham_size_t)sizeof(buffer), rec.size);
          BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
        }
      }
    }

    for (i = 0; i < MAX_DB; i++)
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void multiDbInsertFindExtendedEraseTest() {
    int i;
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ham_env_t *env;
    ham_db_t *db[MAX_DB];
    ham_record_t rec;
    ham_key_t key;
    char buffer[512];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));

      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        memset(buffer, (char)value, sizeof(buffer));
        key.data = buffer;
        key.size = sizeof(buffer);
        rec.data = buffer;
        rec.size = sizeof(buffer);
        sprintf(buffer, "%08x%08x", j, i+1);

        BFC_ASSERT_EQUAL(0, ham_db_insert(db[i], 0, &key, &rec, 0));
      }
    }

    for (i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        memset(buffer, (char)value, sizeof(buffer));
        key.data = buffer;
        key.size = sizeof(buffer);
        sprintf(buffer, "%08x%08x", j, i+1);

        BFC_ASSERT_EQUAL(0, ham_db_find(db[i], 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_size_t)sizeof(buffer), rec.size);
        BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
      }
    }

    for (i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j += 2) { // delete every 2nd entry
        int value = j * (i + 1);
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        memset(buffer, (char)value, sizeof(buffer));
        key.data = buffer;
        key.size = sizeof(buffer);
        sprintf(buffer, "%08x%08x", j, i+1);

        BFC_ASSERT_EQUAL(0, ham_db_erase(db[i], 0, &key, 0));
      }
    }

    if (!(m_flags & HAM_IN_MEMORY)) {
      for (i = 0; i < MAX_DB; i++) {
        BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db[i],
              (ham_u16_t)i + 1, 0, 0));
        for (int j = 0; j < MAX_ITEMS; j++) {
          int value = j * (i + 1);
          memset(&key, 0, sizeof(key));
          memset(&rec, 0, sizeof(rec));
          memset(buffer, (char)value, sizeof(buffer));
          key.data = buffer;
          key.size = sizeof(buffer);
          sprintf(buffer, "%08x%08x", j, i+1);

          if (j & 1) { // must exist
            BFC_ASSERT_EQUAL(0,
                ham_db_find(db[i], 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_size_t)sizeof(buffer), rec.size);
            BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
          }
          else { // was deleted
            BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_db_find(db[i], 0, &key, &rec, 0));
          }
        }
      }
    }

    for (i = 0; i < MAX_DB; i++)
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void multiDbInsertCursorTest() {
    int i;
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ham_env_t *env;
    ham_db_t *db[MAX_DB];
    ham_cursor_t *cursor[MAX_DB];
    ham_record_t rec;
    ham_key_t key;
    char buffer[512];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));
      BFC_ASSERT_EQUAL(0, ham_cursor_create(db[i], 0, 0, &cursor[i]));

      for (int j = 0; j < MAX_ITEMS; j++) {
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        sprintf(buffer, "%08x%08x", j, i+1);
        key.data = buffer;
        key.size = (ham_u16_t)strlen(buffer) + 1;
        rec.data = buffer;
        rec.size = (ham_u16_t)strlen(buffer) + 1;

        BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor[i], &key, &rec, 0));
      }
    }

    for (i = 0; i < MAX_DB; i++) {
      memset(&key, 0, sizeof(key));
      memset(&rec, 0, sizeof(rec));

      BFC_ASSERT_EQUAL(0, ham_cursor_move(cursor[i], &key,
            &rec, HAM_CURSOR_FIRST));
      sprintf(buffer, "%08x%08x", 0, i+1);
      BFC_ASSERT_EQUAL((ham_size_t)strlen(buffer) + 1, rec.size);
      BFC_ASSERT_EQUAL(0, strcmp(buffer, (char *)rec.data));

      for (int j = 1; j < MAX_ITEMS; j++) {
        BFC_ASSERT_EQUAL(0, ham_cursor_move(cursor[i], &key,
            &rec, HAM_CURSOR_NEXT));
        sprintf(buffer, "%08x%08x", j, i+1);
        BFC_ASSERT_EQUAL((ham_size_t)strlen(buffer) + 1, rec.size);
        BFC_ASSERT_EQUAL(0, strcmp(buffer, (char *)rec.data));
      }
    }

    for (i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j += 2) { // delete every 2nd entry
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        sprintf(buffer, "%08x%08x", j, i + 1);
        key.data = buffer;
        key.size = (ham_u16_t)strlen(buffer) + 1;

        BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor[i], &key, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(cursor[i], 0));
      }
    }

    if (!(m_flags & HAM_IN_MEMORY)) {
      for (i = 0; i < MAX_DB; i++) {
        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor[i]));
        BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db[i],
              (ham_u16_t)i + 1, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(db[i], 0, 0, &cursor[i]));
        for (int j = 0; j < MAX_ITEMS; j++) {
          memset(&key, 0, sizeof(key));
          memset(&rec, 0, sizeof(rec));
          sprintf(buffer, "%08x%08x", j, i + 1);
          key.data = buffer;
          key.size = (ham_u16_t)strlen(buffer) + 1;

          if (j & 1) { // must exist
            BFC_ASSERT_EQUAL(0,
                ham_cursor_find(cursor[i], &key, 0, 0));
            BFC_ASSERT_EQUAL(0,
                ham_cursor_move(cursor[i], 0, &rec, 0));
            BFC_ASSERT_EQUAL((ham_size_t)strlen(buffer) + 1, rec.size);
            BFC_ASSERT_EQUAL(0, strcmp(buffer, (char *)rec.data));
          }
          else { // was deleted
            BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_find(cursor[i], &key, 0, 0));
          }
        }
      }
    }

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor[i]));
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
    }

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void multiDbInsertFindExtendedCloseReopenTest() {
    int i;
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ham_env_t *env;
    ham_db_t *db[MAX_DB];
    ham_record_t rec;
    ham_key_t key;
    char buffer[512];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++)
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i+1, 0, 0));

    for (i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        memset(buffer, (char)value, sizeof(buffer));
        key.data = buffer;
        key.size = sizeof(buffer);
        rec.data = buffer;
        rec.size = sizeof(buffer);
        sprintf(buffer, "%08x%08x", j, i+1);

        BFC_ASSERT_EQUAL(0, ham_db_insert(db[i], 0, &key, &rec, 0));
      }
      if (!(m_flags & HAM_IN_MEMORY))
        BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
    }

    if (!(m_flags & HAM_IN_MEMORY)) {
      BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
      BFC_ASSERT_EQUAL(0, ham_env_open(&env, BFC_OPATH(".test"), m_flags, 0));
    }

    for (i = 0; i < MAX_DB; i++) {
      if (!(m_flags & HAM_IN_MEMORY)) {
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db[i],
              (ham_u16_t)i+1, 0, 0));
      }
      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        memset(buffer, (char)value, sizeof(buffer));
        key.data = buffer;
        key.size = sizeof(buffer);
        sprintf(buffer, "%08x%08x", j, i+1);

        BFC_ASSERT_EQUAL(0, ham_db_find(db[i], 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_size_t)sizeof(buffer), rec.size);
        BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
      }
    }

    for (i = 0; i < MAX_DB; i++)
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void renameOpenDatabases() {
    int i;
    const int MAX_DB = 10;
    ham_env_t *env;
    ham_db_t *db[MAX_DB];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++)
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i+1, 0, 0));

    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_rename_db(0, 1, 2, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_rename_db(env, 0, 2, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_rename_db(env, 1, 0, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_rename_db(env, 1, 0xffff, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_rename_db(env, 1, 1, 0));
    BFC_ASSERT_EQUAL(HAM_DATABASE_ALREADY_EXISTS,
        ham_env_rename_db(env, 1, 5, 0));
    BFC_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND,
        ham_env_rename_db(env, 1000, 20, 0));

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(0, ham_env_rename_db(env,
            (ham_u16_t)i + 1, (ham_u16_t)i + 1000, 0));
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
    }

    if (!(m_flags & HAM_IN_MEMORY)) {
      for (i = 0; i < MAX_DB; i++) {
        BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db[i],
              (ham_u16_t)i+1000, 0, 0));
      }

      for (i = 0; i < MAX_DB; i++)
        BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
    }

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void renameClosedDatabases() {
    int i;
    const int MAX_DB = 10;
    ham_env_t *env;
    ham_db_t *db[MAX_DB];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
    }

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(0, ham_env_rename_db(env,
            (ham_u16_t)i + 1, (ham_u16_t)i + 1000, 0));
    }

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db[i],
            (ham_u16_t)i + 1000, 0, 0));
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
    }

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void eraseOpenDatabases() {
    int i;
    const int MAX_DB = 1;
    ham_env_t *env;
    ham_db_t *db[MAX_DB];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));
    }

    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
            ham_env_erase_db(0, (ham_u16_t)i + 1, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
            ham_env_erase_db(env, 0, 0));

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(HAM_DATABASE_ALREADY_OPEN,
              ham_env_erase_db(env, (ham_u16_t)i + 1, 0));
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
      if (m_flags & HAM_IN_MEMORY) {
        BFC_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND,
            ham_env_erase_db(env, (ham_u16_t)i + 1, 0));
      }
      else {
        BFC_ASSERT_EQUAL(0,
            ham_env_erase_db(env, (ham_u16_t)i + 1, 0));
      }
    }

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void eraseUnknownDatabases() {
    int i;
    const int MAX_DB = 1;
    ham_env_t *env;
    ham_db_t *db[MAX_DB];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));
    }

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND,
              ham_env_erase_db(env, (ham_u16_t)i + 1000, 0));
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
      BFC_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND,
              ham_env_erase_db(env, (ham_u16_t)i + 1000, 0));
    }

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void eraseMultipleDatabases() {
    int i, j;
    const int MAX_DB = 13;
    const int MAX_ITEMS = 300;
    ham_env_t *env;
    ham_db_t *db[MAX_DB];
    ham_record_t rec;
    ham_key_t key;
    char buffer[512];
    ham_parameter_t ps[] = {
      { HAM_PARAM_PAGESIZE, 1024 * 6 },
      { 0, 0 }
    };
    ham_parameter_t ps2[] = {
      { HAM_PARAM_KEYSIZE, sizeof(buffer) },
      { 0, 0 }
    };

    BFC_ASSERT_EQUAL(0,
      ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, ps));

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL_I(0,
        ham_env_create_db(env, &db[i], (ham_u16_t)i + 1, 0, ps2), i);
      for (j = 0; j < MAX_ITEMS; j++) {
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        memset(buffer, 0, sizeof(buffer));
        sprintf(buffer, "%08x%08x", j, i+1);
        key.data = buffer;
        key.size = sizeof(buffer);
        key.flags = HAM_KEY_USER_ALLOC;
        rec.data = buffer;
        rec.size = sizeof(buffer);
        rec.flags = HAM_RECORD_USER_ALLOC;

        BFC_ASSERT_EQUAL_I(0,
            ham_db_insert(db[i], 0, &key, &rec, 0), j + i * MAX_ITEMS);
      }
      BFC_ASSERT_EQUAL_I(0, ham_db_close(db[i], 0), i);
    }

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(((m_flags & HAM_IN_MEMORY)
                ? HAM_DATABASE_NOT_FOUND
                : 0),
        ham_env_erase_db(env, (ham_u16_t)i + 1, 0));
    }

    for (i = 0; i < 10; i++) {
      BFC_ASSERT_EQUAL(((m_flags & HAM_IN_MEMORY)
                ? HAM_INV_PARAMETER
                : HAM_DATABASE_NOT_FOUND),
        ham_env_open_db(env, &db[i], (ham_u16_t)i + 1, 0, 0));
    }

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void eraseMultipleDatabasesReopenEnv() {
    int i, j;
    const int MAX_DB = 13;
    const int MAX_ITEMS = 300;
    ham_env_t *env;
    ham_db_t *db[MAX_DB];
    ham_record_t rec;
    ham_key_t key;
    char buffer[512];

    BFC_ASSERT_EQUAL(0,
      ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL_I(0,
        ham_env_create_db(env, &db[i], (ham_u16_t)i+1, 0, 0), i);
      for (j = 0; j < MAX_ITEMS; j++) {
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        memset(buffer, 0, sizeof(buffer));
        sprintf(buffer, "%08x%08x", j, i+1);
        key.data = buffer;
        key.size = sizeof(buffer);
        key.flags = HAM_KEY_USER_ALLOC;
        rec.data = buffer;
        rec.size = sizeof(buffer);
        rec.flags = HAM_RECORD_USER_ALLOC;

        BFC_ASSERT_EQUAL_I(0,
          ham_db_insert(db[i], 0, &key, &rec, 0), j+i*MAX_ITEMS);
      }
      BFC_ASSERT_EQUAL_I(0, ham_db_close(db[i], 0), i);
    }

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
    BFC_ASSERT_EQUAL(0,
      ham_env_open(&env, BFC_OPATH(".test"), m_flags, 0));

    for (i = 0; i < MAX_DB; i++) {
      BFC_ASSERT_EQUAL(0,
        ham_env_erase_db(env, (ham_u16_t)i + 1, 0));
    }

    for (i = 0; i < 10; i++) {
      BFC_ASSERT_EQUAL(HAM_DATABASE_NOT_FOUND,
        ham_env_open_db(env, &db[i], (ham_u16_t)i + 1, 0, 0));
    }

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void endianTestOpenDatabase() {
// i currently don't have access to a big-endian machine, and the existing
// files were created with a database < 1.0.9 and are no longer supported
#if 0
    // created by running sample env2
#if defined(HAM_LITTLE_ENDIAN)
    BFC_ASSERT_EQUAL(0, ham_env_open(&env,
          BFC_IPATH("data/env-endian-test-open-database-be.hdb"), 0, 0));
#else
    BFC_ASSERT_EQUAL(0, ham_env_open(&env,
          BFC_IPATH("data/env-endian-test-open-database-le.hdb"), 0, 0));
#endif
    BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db, 1, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));
    BFC_ASSERT_EQUAL(0, ham_env_open_db(env, &db, 2, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_close(db, 0));
#endif
  }

  void limitsReachedTest() {
    int i;
    const int MAX_DB = DB_MAX_INDICES + 1;
    ham_env_t *env;
    ham_db_t *db[MAX_DB];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB - 1; i++)
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));

    BFC_ASSERT_EQUAL(HAM_LIMITS_REACHED,
        ham_env_create_db(env, &db[i], (ham_u16_t)i + 1, 0, 0));

    for (i = 0; i < MAX_DB - 1; i++)
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void getDatabaseNamesTest() {
    ham_env_t *env;
    ham_db_t *db1, *db2, *db3;
    ham_u16_t names[5];
    ham_size_t names_size = 0;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_get_database_names(0, names, &names_size));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_get_database_names(env, 0, &names_size));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_get_database_names(env, names, 0));

    names_size = 1;
    BFC_ASSERT_EQUAL(0,
        ham_env_get_database_names(env, names, &names_size));
    BFC_ASSERT_EQUAL((ham_size_t)0, names_size);

    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db1, 111, 0, 0));
    names_size = 0;
    BFC_ASSERT_EQUAL(HAM_LIMITS_REACHED,
        ham_env_get_database_names(env, names, &names_size));

    names_size = 1;
    BFC_ASSERT_EQUAL(0,
        ham_env_get_database_names(env, names, &names_size));
    BFC_ASSERT_EQUAL((ham_size_t)1, names_size);
    BFC_ASSERT_EQUAL((ham_u16_t)111, names[0]);

    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db2, 222, 0, 0));
    names_size = 1;
    BFC_ASSERT_EQUAL(HAM_LIMITS_REACHED,
        ham_env_get_database_names(env, names, &names_size));

    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db3, 333, 0, 0));
    names_size = 5;
    BFC_ASSERT_EQUAL(0,
        ham_env_get_database_names(env, names, &names_size));
    BFC_ASSERT_EQUAL((ham_size_t)3, names_size);
    BFC_ASSERT_EQUAL((ham_u16_t)111, names[0]);
    BFC_ASSERT_EQUAL((ham_u16_t)222, names[1]);
    BFC_ASSERT_EQUAL((ham_u16_t)333, names[2]);

    BFC_ASSERT_EQUAL(0, ham_db_close(db2, 0));
    if (!(m_flags & HAM_IN_MEMORY)) {
      BFC_ASSERT_EQUAL(0, ham_env_erase_db(env, 222, 0));
      names_size = 5;
      BFC_ASSERT_EQUAL(0,
          ham_env_get_database_names(env, names, &names_size));
      BFC_ASSERT_EQUAL((ham_size_t)2, names_size);
      BFC_ASSERT_EQUAL((ham_u16_t)111, names[0]);
      BFC_ASSERT_EQUAL((ham_u16_t)333, names[1]);
    }

    BFC_ASSERT_EQUAL(0, ham_db_close(db1, 0));
    BFC_ASSERT_EQUAL(0, ham_db_close(db3, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }

  void maxDatabasesTest() {
    ham_env_t *env;
    ham_parameter_t ps[] = { { HAM_PARAM_MAX_DATABASES, 0 }, { 0, 0 } };

    ps[0].value = 0;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, ps));

    ps[0].value = 5;
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, ps));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

    if (os_get_pagesize() == 1024 * 16 || m_flags & HAM_IN_MEMORY) {
      ps[0].value = 493;
      BFC_ASSERT_EQUAL(0,
          ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, ps));
      BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

      ps[0].value = 507;
      BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
          ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, ps));
    }
    else if (os_get_pagesize() == 1024 * 64) {
      ps[0].value = 2029;
      BFC_ASSERT_EQUAL(0,
          ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, ps));
      BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

      ps[0].value = 2030;
      BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
          ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, ps));
    }
  }

  void maxDatabasesReopenTest() {
    ham_env_t *env;
    ham_db_t *db;
    ham_parameter_t ps[] = { { HAM_PARAM_MAX_DATABASES, 50 }, { 0, 0 }};

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, ps));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 333, 0, 0));
    if (!(m_flags & HAM_IN_MEMORY)) {
      BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

      BFC_ASSERT_EQUAL(0,
          ham_env_open(&env, BFC_OPATH(".test"), m_flags, 0));
      BFC_ASSERT_EQUAL(0,
          ham_env_open_db(env, &db, 333, 0, 0));
    }
    BFC_ASSERT_EQUAL(50, ((Environment *)env)->get_max_databases());
    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void createOpenEmptyTest() {
    ham_env_t *env;
    ham_db_t *db[10];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));
    for (int i = 0; i < 10; i++)
      BFC_ASSERT_EQUAL(0,
          ham_env_create_db(env, &db[i], 333+i, 0, 0));
    if (!(m_flags & HAM_IN_MEMORY)) {
      BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));

      BFC_ASSERT_EQUAL(0,
          ham_env_open(&env, BFC_OPATH(".test"), m_flags, 0));
      for (int i = 0; i < 10; i++) {
        BFC_ASSERT_EQUAL(0,
          ham_env_open_db(env, &db[i], 333+i, 0, 0));
      }
    }
    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }
};

class InMemoryEnvTest : public EnvTest {
public:
  InMemoryEnvTest()
    : EnvTest(HAM_IN_MEMORY, "InMemoryEnvTest") {
    clear_tests(); // don't inherit tests
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(InMemoryEnvTest, createCloseTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, createCloseOpenCloseTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, createCloseOpenCloseWithDatabasesTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, createPagesizeReopenTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, createWithKeysizeTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, createDbWithKeysizeTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, createAndOpenMultiDbTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, disableVarkeyTests);
    BFC_REGISTER_TEST(InMemoryEnvTest, autoCleanupTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, autoCleanup2Test);
    BFC_REGISTER_TEST(InMemoryEnvTest, memoryDbTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, multiDbTest2);
    BFC_REGISTER_TEST(InMemoryEnvTest, multiDbInsertFindTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, multiDbInsertFindExtendedTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, multiDbInsertFindExtendedEraseTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, multiDbInsertCursorTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, multiDbInsertFindExtendedCloseReopenTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, renameOpenDatabases);
    BFC_REGISTER_TEST(InMemoryEnvTest, eraseOpenDatabases);
    BFC_REGISTER_TEST(InMemoryEnvTest, eraseUnknownDatabases);
    //BFC_REGISTER_TEST(InMemoryEnvTest, eraseMultipleDatabases);
    BFC_REGISTER_TEST(InMemoryEnvTest, limitsReachedTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, getDatabaseNamesTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, maxDatabasesTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, maxDatabasesReopenTest);
    BFC_REGISTER_TEST(InMemoryEnvTest, createOpenEmptyTest);
  }

public:
  void memoryDbTest() {
    int i;
    ham_env_t *env;
    ham_db_t *db[10];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test"), m_flags, 0664, 0));

    for (i = 0; i < 10; i++)
      BFC_ASSERT_EQUAL(0, ham_env_create_db(env, &db[i],
            (ham_u16_t)i + 1, 0, 0));

    for (i = 0; i < 10; i++)
      BFC_ASSERT_EQUAL(0, ham_db_close(db[i], 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
  }
};

BFC_REGISTER_FIXTURE(EnvTest);
BFC_REGISTER_FIXTURE(InMemoryEnvTest);
