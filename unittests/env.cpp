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

#include "3rdparty/catch/catch.hpp"

#include <stdint.h>

#include "4db/db_local.h"
#include "4env/env_local.h"

#include "os.hpp"
#include "fixture.hpp"

using namespace upscaledb;

struct EnvFixture {
  uint32_t m_flags;

  EnvFixture(uint32_t flags = 0)
    : m_flags(flags) {
  }

  void createCloseTest() {
    BaseFixture bf;
    bf.require_create(m_flags)
      .close();

    REQUIRE(UPS_INV_PARAMETER == ups_env_close(0, 0));
  }

  void createCloseOpenCloseTest() {
    BaseFixture bf;
    bf.require_create(m_flags)
      .close();
    if (NOTSET(m_flags, UPS_IN_MEMORY))
      bf.require_open(m_flags);
  }

  void createCloseOpenCloseWithDatabasesTest() {
    BaseFixture bf;
    bf.require_create(m_flags);


    ups_db_t *db, *db2;

    REQUIRE(UPS_INV_PARAMETER == ups_env_create_db(0, &db, 333, 0, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_env_create_db(bf.env, 0, 333, 0, 0));
    REQUIRE(0 == ups_env_create_db(bf.env, &db, 333, 0, 0));
    REQUIRE(UPS_DATABASE_ALREADY_EXISTS ==
        ups_env_create_db(bf.env, &db2, 333, 0, 0));
    REQUIRE(0 == ups_db_close(db, 0));

    REQUIRE(UPS_INV_PARAMETER == ups_env_open_db(0, &db, 333, 0, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_env_open_db(bf.env, 0, 333, 0, 0));

    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      REQUIRE(0 == ups_env_open_db(bf.env, &db, 333, 0, 0));
      REQUIRE(UPS_DATABASE_ALREADY_OPEN ==
          ups_env_open_db(bf.env, &db, 333, 0, 0));

      bf.close()
        .require_open();
    }
  }

  void createCloseEmptyOpenCloseWithDatabasesTest() {
    ups_parameter_t parameters[] = {
       { UPS_PARAM_CACHESIZE, 128 * 1024 },
       { UPS_PARAM_PAGESIZE, 64 * 1024 },
       { 0, 0 }
    };
    ups_parameter_t parameters2[] = {
       { UPS_PARAM_CACHESIZE, 128 * 1024 },
       { 0, 0 }
    };

    ups_db_t *db[128], *dbx;

    BaseFixture bf;
    bf.require_create(m_flags, parameters)
      .require_parameter(UPS_PARAM_CACHESIZE, 1024 * 128)
      .require_parameter(UPS_PARAM_PAGESIZE, 1024 * 64)
      .require_parameter(UPS_PARAM_MAX_DATABASES, 2179);
    
    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      bf.close()
        .require_open(m_flags, parameters2);
    }

    bf.require_parameter(UPS_PARAM_CACHESIZE, 1024 * 128)
      .require_parameter(UPS_PARAM_PAGESIZE, 1024 * 64)
      .require_parameter(UPS_PARAM_MAX_DATABASES, 2179);

    // now create 128 DBs
    for (int i = 0; i < 128; i++) {
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], i + 100, 0, 0));
      REQUIRE(UPS_DATABASE_ALREADY_EXISTS ==
          ups_env_create_db(bf.env, &dbx, i + 100, 0, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
      REQUIRE(0 == ups_env_open_db(bf.env, &db[i], i + 100, 0, 0));
    }
  }

  void autoCleanupTest() {
    BaseFixture bf;
    ups_db_t *db[3];
    ups_cursor_t *c[5];

    bf.create_env(m_flags);
    for (int i = 0; i < 3; i++)
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], i + 1, 0, 0));
    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], db[0], 0, 0));

    // cleans up when going out of scope
  }

  void autoCleanup2Test() {
    BaseFixture bf;
    bf.require_create(m_flags);
    // cleans up when going out of scope
  }

  void readOnlyTest() {
    ups_db_t *db2;
    ups_key_t key = {0};
    ups_record_t rec = {0};
    ups_cursor_t *cursor;

    BaseFixture bf;
    bf.require_create(0)
      .close()
      .require_open(UPS_READ_ONLY);

    REQUIRE(0 == ups_cursor_create(&cursor, bf.db, 0, 0));
    REQUIRE(UPS_DATABASE_ALREADY_OPEN ==
        ups_env_open_db(bf.env, &db2, 1, 0, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_env_create_db(bf.env, &db2, 444, 0, 0));

    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_db_insert(bf.db, 0, &key, &rec, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_db_erase(bf.db, 0, &key, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_cursor_overwrite(cursor, &rec, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_cursor_erase(cursor, 0));
  }

  void createPagesizeReopenTest() {
    ups_parameter_t ps[] = { { UPS_PARAM_PAGESIZE, 1024 * 128 }, { 0, 0 } };

    BaseFixture bf;
    bf.require_create(m_flags, ps);

    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      bf.close()
        .require_open(m_flags);
    }
  }

  void openFailCloseTest() {
    ups_env_t *env;

    REQUIRE(UPS_FILE_NOT_FOUND == ups_env_open(&env, "xxxxxx...", 0, 0));
    REQUIRE(env == nullptr);
  }

  void openWithKeysizeTest() {
    ups_env_t *env;

    REQUIRE(UPS_INV_PARAMETER == ups_env_open(0, "test.db", m_flags, 0));
    REQUIRE(UPS_FILE_NOT_FOUND == ups_env_open(&env, "xxxtest.db", m_flags, 0));
  }

  void createDbWithKeysizeTest() {
    ups_parameter_t params[] = {
       { UPS_PARAM_KEYSIZE, (uint64_t)64 },
       { 0, 0 }
    };

    BaseFixture bf;
    bf.require_create(m_flags, 0, 0, params);
    REQUIRE(bf.ldb()->config.key_size == 64);
  }

  // check to make sure create and open support accessing more
  // than DB_MAX_INDICES DBs in one env:
  void createAndOpenMultiDbTest() {
    const int MAX = 256;
    ups_db_t *db[MAX];
    ups_parameter_t parameters[] = {
       { UPS_PARAM_KEYSIZE, 20 },
       { 0, 0 }
    };
    ups_parameter_t parameters2[] = {
       { UPS_PARAM_CACHESIZE, 1024 * 128 },
       { UPS_PARAM_PAGESIZE, 1024 * 4 },
       { 0, 0 }
    };
    ups_parameter_t parameters3[] = {
       { UPS_PARAM_CACHESIZE, 1024 * 128 },
       { 0, 0 }
    };

    BaseFixture bf;

    if (ISSET(m_flags, UPS_IN_MEMORY)) {
      REQUIRE(UPS_INV_PARAMETER == ups_env_create(&bf.env, "test.db",
            m_flags, 0644, parameters2));
      parameters2[0].value = 0; // cache_size := 0
      parameters2[1].value = 0; // page_size = 0
    }
    else {
      REQUIRE(UPS_INV_PARAMETER == ups_env_create(&bf.env, "test.db",
            m_flags | UPS_CACHE_UNLIMITED, 0644, parameters2));
      parameters2[1].value = 65536; // page_size = 64K
    }

    REQUIRE(0 == bf.create_env(m_flags, parameters2));

    char buffer[20] = {0};

    // create DBs
    for (int i = 0; i < MAX; i++) {
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], i + 1, 0, parameters));

      ups_key_t key = ups_make_key(buffer, sizeof(buffer));
      ups_record_t record = ups_make_record(buffer, sizeof(buffer));

      *(int *)&buffer[0] = i;
      REQUIRE(0 == ups_db_insert(db[i], 0, &key, &record, 0));
    }

    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      bf.close();

      // open DBs
      // page_size param not allowed
      REQUIRE(UPS_INV_PARAMETER == ups_env_open(&bf.env, "test.db",
                      m_flags, parameters2));
      REQUIRE(0 == bf.open_env(m_flags, parameters3));
    }

    for (int i = 0; i < MAX; i++) {
      if (NOTSET(m_flags, UPS_IN_MEMORY))
        REQUIRE(0 == ups_env_open_db(bf.env, &db[i], i + 1, 0, 0));
      
      ups_key_t key = ups_make_key(buffer, sizeof(buffer));
      ups_record_t record = {0};
      *(int *)&buffer[0] = i;

      REQUIRE(0 == ups_db_find(db[i], 0, &key, &record, 0));
      REQUIRE(*(int *)key.data == i);
      REQUIRE(*(int *)record.data == i);
    }
  }

  void multiDbTest() {
    BaseFixture bf;
    bf.create_env(m_flags);
    ups_db_t *db[10];

    for (int i = 0; i < 10; i++) {
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
      REQUIRE(0 == ups_env_open_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    for (int i = 0; i < 10; i++) {
      REQUIRE(0 == ups_env_open_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
    }
  }

  void multiDbTest2() {
    BaseFixture bf;
    bf.create_env(m_flags);
    ups_db_t *db[10];

    for (int i = 0; i < 10; i++)
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
    for (int i = 0; i < 10; i++)
      REQUIRE(0 == ups_db_close(db[i], 0));
    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      for (int i = 0; i < 10; i++) {
        REQUIRE(0 == ups_env_open_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
        REQUIRE(0 == ups_db_close(db[i], 0));
      }
    }
  }

  void multiDbInsertFindTest() {
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ups_db_t *db[MAX_DB];

    BaseFixture bf;
    bf.create_env(m_flags);

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));

      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        ups_key_t key = ups_make_key(&value, sizeof(value));
        ups_record_t rec = ups_make_record(&value, sizeof(value));
        REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
      }
    }

    for (int i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        ups_key_t key = ups_make_key(&value, sizeof(value));
        ups_record_t rec = {0};

        REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
        REQUIRE(value == *(int *)rec.data);
        REQUIRE((uint16_t)sizeof(value) == rec.size);
      }
    }

    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      for (int i = 0; i < MAX_DB; i++) {
        REQUIRE(0 == ups_db_close(db[i], 0));
        REQUIRE(0 == ups_env_open_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
        for (int j = 0; j < MAX_ITEMS; j++) {
          int value = j * (i + 1);
          ups_key_t key = ups_make_key(&value, sizeof(value));
          ups_record_t rec = {0};

          REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
          REQUIRE(value == *(int *)rec.data);
          REQUIRE((uint16_t)sizeof(value) == rec.size);
        }
      }
    }
  }

  void multiDbInsertFindExtendedTest() {
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ups_db_t *db[MAX_DB];
    char buffer[512] = {0};

    BaseFixture bf;
    bf.create_env(m_flags);

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));

      for (int j = 0; j < MAX_ITEMS; j++) {
        ::sprintf(buffer, "%08x%08x", j, i + 1);
        ups_key_t key = ups_make_key(&buffer, sizeof(buffer));
        ups_record_t rec = ups_make_record(&buffer, sizeof(buffer));
        REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
      }
    }

    for (int i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j++) {
        ::sprintf(buffer, "%08x%08x", j, i + 1);
        ups_key_t key = ups_make_key(&buffer, sizeof(buffer));
        ups_record_t rec = {0};
        REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
        REQUIRE((uint32_t)sizeof(buffer) == rec.size);
        REQUIRE(0 == ::memcmp(buffer, rec.data, rec.size));
      }
    }

    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      for (int i = 0; i < MAX_DB; i++) {
        REQUIRE(0 == ups_db_close(db[i], 0));
        REQUIRE(0 == ups_env_open_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
        for (int j = 0; j < MAX_ITEMS; j++) {
          ::sprintf(buffer, "%08x%08x", j, i + 1);
          ups_key_t key = ups_make_key(&buffer, sizeof(buffer));
          ups_record_t rec = {0};
          REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
          REQUIRE((uint32_t)sizeof(buffer) == rec.size);
          REQUIRE(0 == ::memcmp(buffer, rec.data, rec.size));
        }
      }
    }
  }

  void multiDbInsertFindExtendedEraseTest() {
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ups_db_t *db[MAX_DB];
    char buffer[512];

    BaseFixture bf;
    bf.create_env(m_flags);

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));

      for (int j = 0; j < MAX_ITEMS; j++) {
        ::sprintf(buffer, "%08x%08x", j, i + 1);
        ups_key_t key = ups_make_key(&buffer, sizeof(buffer));
        ups_record_t rec = ups_make_record(&buffer, sizeof(buffer));
        REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
      }
    }

    for (int i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j++) {
        ::sprintf(buffer, "%08x%08x", j, i + 1);
        ups_key_t key = ups_make_key(&buffer, sizeof(buffer));
        ups_record_t rec = {0};
        REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
        REQUIRE((uint32_t)sizeof(buffer) == rec.size);
        REQUIRE(0 == ::memcmp(buffer, rec.data, rec.size));
      }
    }

    for (int i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j += 2) { // delete every 2nd entry
        ::sprintf(buffer, "%08x%08x", j, i + 1);
        ups_key_t key = ups_make_key(&buffer, sizeof(buffer));
        REQUIRE(0 == ups_db_erase(db[i], 0, &key, 0));
      }
    }

    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      for (int i = 0; i < MAX_DB; i++) {
        REQUIRE(0 == ups_db_close(db[i], 0));
        REQUIRE(0 == ups_env_open_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
        for (int j = 0; j < MAX_ITEMS; j++) {
          ::sprintf(buffer, "%08x%08x", j, i + 1);
          ups_key_t key = ups_make_key(&buffer, sizeof(buffer));
          ups_record_t rec = {0};

          if (j & 1) { // must exist
            REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
            REQUIRE((uint32_t)sizeof(buffer) == rec.size);
            REQUIRE(0 == ::memcmp(buffer, rec.data, rec.size));
          }
          else { // was deleted
            REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db[i], 0, &key, &rec, 0));
          }
        }
      }
    }
  }

  void multiDbInsertCursorTest() {
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ups_db_t *db[MAX_DB];
    ups_cursor_t *cursor[MAX_DB];
    char buffer[512] = {0};

    BaseFixture bf;
    bf.create_env(m_flags);

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
      REQUIRE(0 == ups_cursor_create(&cursor[i], db[i], 0, 0));

      for (int j = 0; j < MAX_ITEMS; j++) {
        sprintf(buffer, "%08x%08x", j, i + 1);
        ups_key_t key = ups_make_key(buffer, (uint16_t)(::strlen(buffer) + 1));
        ups_record_t rec = ups_make_record(buffer,
                        (uint32_t)::strlen(buffer) + 1u);
        REQUIRE(0 == ups_cursor_insert(cursor[i], &key, &rec, 0));
      }
    }

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_cursor_close(cursor[i]));
      REQUIRE(0 == ups_cursor_create(&cursor[i], db[i], 0, 0));
      for (int j = 0; j < MAX_ITEMS; j++) {
        ::sprintf(buffer, "%08x%08x", j, i + 1);
        ups_key_t key = {0};
        ups_record_t rec = {0};

        REQUIRE(0 == ups_cursor_move(cursor[i], &key, &rec, UPS_CURSOR_NEXT));
        REQUIRE((uint32_t)(::strlen(buffer) + 1) == rec.size);
        REQUIRE(0 == ::strcmp(buffer, (char *)rec.data));
      }
    }

    for (int i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j += 2) { // delete every 2nd entry
        ::sprintf(buffer, "%08x%08x", j, i + 1);
        ups_key_t key = ups_make_key(buffer, (uint16_t)(::strlen(buffer) + 1));

        REQUIRE(0 == ups_cursor_find(cursor[i], &key, 0, 0));
        REQUIRE(0 == ups_cursor_erase(cursor[i], 0));
      }
    }

    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      for (int i = 0; i < MAX_DB; i++) {
        REQUIRE(0 == ups_cursor_close(cursor[i]));
        REQUIRE(0 == ups_db_close(db[i], 0));
        REQUIRE(0 == ups_env_open_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
        REQUIRE(0 == ups_cursor_create(&cursor[i], db[i], 0, 0));

        for (int j = 0; j < MAX_ITEMS; j++) {
          ::sprintf(buffer, "%08x%08x", j, i + 1);
          ups_key_t key = ups_make_key(buffer, (uint16_t)(::strlen(buffer) + 1));
          ups_record_t rec = {0};

          if (j & 1) { // must exist
            REQUIRE(0 == ups_cursor_find(cursor[i], &key, 0, 0));
            REQUIRE(0 == ups_cursor_move(cursor[i], 0, &rec, 0));
            REQUIRE((uint32_t)(::strlen(buffer) + 1) == rec.size);
            REQUIRE(0 == ::strcmp(buffer, (char *)rec.data));
          }
          else { // was deleted
            REQUIRE(UPS_KEY_NOT_FOUND ==
                ups_cursor_find(cursor[i], &key, 0, 0));
          }
        }
      }
    }
  }

  void multiDbInsertFindExtendedCloseReopenTest() {
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ups_db_t *db[MAX_DB];
    char buffer[512];

    BaseFixture bf;
    bf.create_env(m_flags);

    for (int i = 0; i < MAX_DB; i++)
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));

    for (int i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j++) {
        ::sprintf(buffer, "%08x%08x", j, i + 1);
        ups_key_t key = ups_make_key(buffer, (uint16_t)(::strlen(buffer) + 1));
        ups_record_t rec = ups_make_record(buffer,
                        (uint32_t)::strlen(buffer) + 1u);

        REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
      }
      if (NOTSET(m_flags, UPS_IN_MEMORY))
        REQUIRE(0 == ups_db_close(db[i], 0));
    }

    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      bf.close();
      REQUIRE(0 == bf.open_env(m_flags));
    }

    for (int i = 0; i < MAX_DB; i++) {
      if (NOTSET(m_flags, UPS_IN_MEMORY))
        REQUIRE(0 == ups_env_open_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
      for (int j = 0; j < MAX_ITEMS; j++) {
        ::sprintf(buffer, "%08x%08x", j, i + 1);
        ups_key_t key = ups_make_key(buffer, (uint16_t)(::strlen(buffer) + 1));
        ups_record_t rec = {0};

		REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
        REQUIRE(rec.size == ::strlen(buffer) + 1);
        REQUIRE(0 == ::memcmp(buffer, rec.data, rec.size));
      }
    }
  }

  void renameOpenDatabases() {
    const int MAX_DB = 10;
    ups_db_t *db[MAX_DB];

    BaseFixture bf;
    bf.create_env(m_flags);

    for (int i = 0; i < MAX_DB; i++)
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));

    REQUIRE(UPS_INV_PARAMETER == ups_env_rename_db(0, 1, 2, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_env_rename_db(bf.env, 0, 2, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_env_rename_db(bf.env, 1, 0, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_env_rename_db(bf.env, 1, 0xffff, 0));
    REQUIRE(0 == ups_env_rename_db(bf.env, 1, 1, 0));
    REQUIRE(UPS_DATABASE_ALREADY_EXISTS == ups_env_rename_db(bf.env, 1, 5, 0));
    REQUIRE(UPS_DATABASE_NOT_FOUND == ups_env_rename_db(bf.env, 1000, 20, 0));

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_rename_db(bf.env,
            (uint16_t)i + 1, (uint16_t)i + 1000, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      for (int i = 0; i < MAX_DB; i++) {
        REQUIRE(0 == ups_env_open_db(bf.env, &db[i], (uint16_t)i+1000, 0, 0));
      }
    }
  }

  void renameClosedDatabases() {
    const int MAX_DB = 10;
    ups_db_t *db[MAX_DB];

    BaseFixture bf;
    REQUIRE(0 == bf.create_env(m_flags));

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_rename_db(bf.env,
            (uint16_t)i + 1, (uint16_t)i + 1000, 0));
    }

    for (int i = 0; i < MAX_DB; i++)
      REQUIRE(0 == ups_env_open_db(bf.env, &db[i], (uint16_t)i + 1000, 0, 0));
  }

  void eraseOpenDatabases() {
    const int MAX_DB = 1;
    ups_db_t *db[MAX_DB];

    BaseFixture bf;
    bf.create_env(m_flags);

    for (int i = 0; i < MAX_DB; i++)
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));

    REQUIRE(UPS_INV_PARAMETER == ups_env_erase_db(0, 1, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_env_erase_db(bf.env, 0, 0));

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE(UPS_DATABASE_ALREADY_OPEN ==
              ups_env_erase_db(bf.env, (uint16_t)i + 1, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
      if (ISSET(m_flags, UPS_IN_MEMORY)) {
        REQUIRE(UPS_DATABASE_NOT_FOUND ==
                        ups_env_erase_db(bf.env, (uint16_t)i + 1, 0));
      }
      else {
        REQUIRE(0 == ups_env_erase_db(bf.env, (uint16_t)i + 1, 0));
      }
    }
  }

  void eraseUnknownDatabases() {
    const int MAX_DB = 1;
    ups_db_t *db[MAX_DB];

    BaseFixture bf;
    bf.create_env(m_flags);

    for (int i = 0; i < MAX_DB; i++)
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE(UPS_DATABASE_NOT_FOUND ==
              ups_env_erase_db(bf.env, (uint16_t)i + 1000, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
      REQUIRE(UPS_DATABASE_NOT_FOUND ==
              ups_env_erase_db(bf.env, (uint16_t)i + 1000, 0));
    }
  }

  void eraseMultipleDatabases() {
    const int MAX_DB = 13;
    const int MAX_ITEMS = 300;
    ups_db_t *db[MAX_DB];
    char buffer[512] = {0};

    ups_parameter_t ps[] = {
      { UPS_PARAM_PAGESIZE, 1024 * 6 },
      { 0, 0 }
    };
    ups_parameter_t ps2[] = {
      { UPS_PARAM_KEYSIZE, sizeof(buffer) },
      { 0, 0 }
    };

    BaseFixture bf;
    REQUIRE(0 == bf.create_env(m_flags, ps));

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, ps2));
      for (int j = 0; j < MAX_ITEMS; j++) {
        ::sprintf(buffer, "%08x%08x", j, i+1);
        ups_key_t key = ups_make_key(buffer, sizeof(buffer));
        ups_record_t rec = ups_make_record(buffer, sizeof(buffer));
        key.flags = UPS_KEY_USER_ALLOC;
        rec.flags = UPS_RECORD_USER_ALLOC;
        REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
      }
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE((ISSET(m_flags, UPS_IN_MEMORY)
                    ? UPS_DATABASE_NOT_FOUND
                    : 0) ==
        ups_env_erase_db(bf.env, (uint16_t)i + 1, 0));
    }

    for (int i = 0; i < 10; i++) {
      REQUIRE((ISSET(m_flags, UPS_IN_MEMORY)
                    ? UPS_INV_PARAMETER
                    : UPS_DATABASE_NOT_FOUND) ==
        ups_env_open_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
    }
  }

  void eraseMultipleDatabasesReopenEnv() {
    const int MAX_DB = 13;
    const int MAX_ITEMS = 300;
    ups_db_t *db[MAX_DB];
    char buffer[512] = {0};

    BaseFixture bf;
    REQUIRE(0 == bf.create_env(m_flags | UPS_DISABLE_RECLAIM_INTERNAL));

    for (int i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
      for (int j = 0; j < MAX_ITEMS; j++) {
        ::sprintf(buffer, "%08x%08x", j, i + 1);
        ups_key_t key = ups_make_key(buffer, sizeof(buffer));
        ups_record_t rec = ups_make_record(buffer, sizeof(buffer));
        key.flags = UPS_KEY_USER_ALLOC;
        rec.flags = UPS_RECORD_USER_ALLOC;

        REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
      }
    }

    bf.close();
    REQUIRE(0 == bf.open_env(m_flags));

    for (int i = 0; i < MAX_DB; i++)
      REQUIRE(0 == ups_env_erase_db(bf.env, (uint16_t)i + 1, 0));

    for (int i = 0; i < 10; i++)
      REQUIRE(UPS_DATABASE_NOT_FOUND ==
                      ups_env_open_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
  }

  void limitsReachedTest() {
    const int MAX_DB = 540 + 1;
    ups_db_t *db[MAX_DB];

    BaseFixture bf;
    bf.create_env(m_flags);

    for (int i = 0; i < MAX_DB - 1; i++)
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));

    REQUIRE(UPS_LIMITS_REACHED == ups_env_create_db(bf.env, &db[0], 999, 0, 0));
  }

  void getDatabaseNamesTest() {
    ups_db_t *db1, *db2, *db3;
    uint16_t names[5];
    uint32_t names_size = 0;

    BaseFixture bf;
    bf.create_env(m_flags);

    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_get_database_names(0, names, &names_size));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_get_database_names(bf.env, 0, &names_size));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_get_database_names(bf.env, names, 0));

    names_size = 1;
    REQUIRE(0 == ups_env_get_database_names(bf.env, names, &names_size));
    REQUIRE(names_size == 0);

    REQUIRE(0 == ups_env_create_db(bf.env, &db1, 111, 0, 0));
    names_size = 0;
    REQUIRE(UPS_LIMITS_REACHED ==
        ups_env_get_database_names(bf.env, names, &names_size));

    names_size = 1;
    REQUIRE(0 == ups_env_get_database_names(bf.env, names, &names_size));
    REQUIRE(names_size == 1);
    REQUIRE(names[0] == 111);

    REQUIRE(0 == ups_env_create_db(bf.env, &db2, 222, 0, 0));
    names_size = 1;
    REQUIRE(UPS_LIMITS_REACHED ==
        ups_env_get_database_names(bf.env, names, &names_size));

    REQUIRE(0 == ups_env_create_db(bf.env, &db3, 333, 0, 0));
    names_size = 5;
    REQUIRE(0 == ups_env_get_database_names(bf.env, names, &names_size));
    REQUIRE(names_size == 3);
    REQUIRE(names[0] == 111);
    REQUIRE(names[1] == 222);
    REQUIRE(names[2] == 333);

    REQUIRE(0 == ups_db_close(db2, 0));
    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      REQUIRE(0 == ups_env_erase_db(bf.env, 222, 0));
      names_size = 5;
      REQUIRE(0 == ups_env_get_database_names(bf.env, names, &names_size));
      REQUIRE(names_size == 2);
      REQUIRE(names[0] == 111);
      REQUIRE(names[1] == 333);
    }
  }

  void createOpenEmptyTest() {
    ups_db_t *db[10];
    BaseFixture bf;
    bf.require_create(m_flags);

    for (int i = 0; i < 10; i++)
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], 333+i, 0, 0));
    if (NOTSET(m_flags, UPS_IN_MEMORY)) {
      bf.close()
        .require_open(m_flags);

      for (int i = 0; i < 10; i++)
        REQUIRE(0 == ups_env_open_db(bf.env, &db[i], 333+i, 0, 0));
    }
  }

  void memoryDbTest() {
    ups_db_t *db[10];
    BaseFixture bf;
    bf.create_env(m_flags);

    for (int i = 0; i < 10; i++)
      REQUIRE(0 == ups_env_create_db(bf.env, &db[i], (uint16_t)i + 1, 0, 0));
  }
};

TEST_CASE("Env/createCloseTest", "")
{
  EnvFixture f;
  f.createCloseTest();
}

TEST_CASE("Env/createCloseOpenCloseTest", "")
{
  EnvFixture f;
  f.createCloseOpenCloseTest();
}

TEST_CASE("Env/createCloseOpenCloseWithDatabasesTest", "")
{
  EnvFixture f;
  f.createCloseOpenCloseWithDatabasesTest();
}

TEST_CASE("Env/createCloseEmptyOpenCloseWithDatabasesTest", "")
{
  EnvFixture f;
  f.createCloseEmptyOpenCloseWithDatabasesTest();
}

TEST_CASE("Env/autoCleanupTest", "")
{
  EnvFixture f;
  f.autoCleanupTest();
}

TEST_CASE("Env/autoCleanup2Test", "")
{
  EnvFixture f;
  f.autoCleanup2Test();
}

TEST_CASE("Env/readOnlyTest", "")
{
  EnvFixture f;
  f.readOnlyTest();
}

TEST_CASE("Env/createPagesizeReopenTest", "")
{
  EnvFixture f;
  f.createPagesizeReopenTest();
}

TEST_CASE("Env/openFailCloseTest", "")
{
  EnvFixture f;
  f.openFailCloseTest();
}

TEST_CASE("Env/openWithKeysizeTest", "")
{
  EnvFixture f;
  f.openWithKeysizeTest();
}

TEST_CASE("Env/createDbWithKeysizeTest", "")
{
  EnvFixture f;
  f.createDbWithKeysizeTest();
}

TEST_CASE("Env/createAndOpenMultiDbTest", "")
{
  EnvFixture f;
  f.createAndOpenMultiDbTest();
}

TEST_CASE("Env/multiDbTest", "")
{
  EnvFixture f;
  f.multiDbTest();
}

TEST_CASE("Env/multiDbTest2", "")
{
  EnvFixture f;
  f.multiDbTest2();
}

TEST_CASE("Env/multiDbInsertFindTest", "")
{
  EnvFixture f;
  f.multiDbInsertFindTest();
}

TEST_CASE("Env/multiDbInsertFindExtendedTest", "")
{
  EnvFixture f;
  f.multiDbInsertFindExtendedTest();
}

TEST_CASE("Env/multiDbInsertFindExtendedEraseTest", "")
{
  EnvFixture f;
  f.multiDbInsertFindExtendedEraseTest();
}

TEST_CASE("Env/multiDbInsertCursorTest", "")
{
  EnvFixture f;
  f.multiDbInsertCursorTest();
}

TEST_CASE("Env/multiDbInsertFindExtendedCloseReopenTest", "")
{
  EnvFixture f;
  f.multiDbInsertFindExtendedCloseReopenTest();
}

TEST_CASE("Env/renameOpenDatabases", "")
{
  EnvFixture f;
  f.renameOpenDatabases();
}

TEST_CASE("Env/renameClosedDatabases", "")
{
  EnvFixture f;
  f.renameClosedDatabases();
}

TEST_CASE("Env/eraseOpenDatabases", "")
{
  EnvFixture f;
  f.eraseOpenDatabases();
}

TEST_CASE("Env/eraseUnknownDatabases", "")
{
  EnvFixture f;
  f.eraseUnknownDatabases();
}

TEST_CASE("Env/eraseMultipleDatabases", "")
{
  EnvFixture f;
  f.eraseMultipleDatabases();
}

TEST_CASE("Env/eraseMultipleDatabasesReopenEnv", "")
{
  EnvFixture f;
  f.eraseMultipleDatabasesReopenEnv();
}

TEST_CASE("Env/limitsReachedTest", "")
{
  EnvFixture f;
  f.limitsReachedTest();
}

TEST_CASE("Env/getDatabaseNamesTest", "")
{
  EnvFixture f;
  f.getDatabaseNamesTest();
}

TEST_CASE("Env/createOpenEmptyTest", "")
{
  EnvFixture f;
  f.createOpenEmptyTest();
}


TEST_CASE("Env/inmem/createCloseTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createCloseTest();
}

TEST_CASE("Env/inmem/createCloseOpenCloseTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createCloseOpenCloseTest();
}

TEST_CASE("Env/inmem/createCloseOpenCloseWithDatabasesTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createCloseOpenCloseWithDatabasesTest();
}

TEST_CASE("Env/inmem/createPagesizeReopenTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createPagesizeReopenTest();
}

TEST_CASE("Env/inmem/createDbWithKeysizeTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createDbWithKeysizeTest();
}

TEST_CASE("Env/inmem/createAndOpenMultiDbTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createAndOpenMultiDbTest();
}

TEST_CASE("Env/inmem/autoCleanupTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.autoCleanupTest();
}

TEST_CASE("Env/inmem/autoCleanup2Test", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.autoCleanup2Test();
}

TEST_CASE("Env/inmem/memoryDbTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.memoryDbTest();
}

TEST_CASE("Env/inmem/multiDbTest2", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.multiDbTest2();
}

TEST_CASE("Env/inmem/multiDbInsertFindTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.multiDbInsertFindTest();
}

TEST_CASE("Env/inmem/multiDbInsertFindExtendedTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.multiDbInsertFindExtendedTest();
}

TEST_CASE("Env/inmem/multiDbInsertFindExtendedEraseTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.multiDbInsertFindExtendedEraseTest();
}

TEST_CASE("Env/inmem/multiDbInsertCursorTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.multiDbInsertCursorTest();
}

TEST_CASE("Env/inmem/multiDbInsertFindExtendedCloseReopenTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.multiDbInsertFindExtendedCloseReopenTest();
}

TEST_CASE("Env/inmem/renameOpenDatabases", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.renameOpenDatabases();
}

TEST_CASE("Env/inmem/eraseOpenDatabases", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.eraseOpenDatabases();
}

TEST_CASE("Env/inmem/eraseUnknownDatabases", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.eraseUnknownDatabases();
}

TEST_CASE("Env/inmem/limitsReachedTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.limitsReachedTest();
}

TEST_CASE("Env/inmem/getDatabaseNamesTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.getDatabaseNamesTest();
}

TEST_CASE("Env/inmem/createOpenEmptyTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createOpenEmptyTest();
}

