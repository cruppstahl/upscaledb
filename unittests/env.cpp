/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#include "3rdparty/catch/catch.hpp"

#include <stdint.h>

#include "2page/page.h"
#include "4db/db_local.h"
#include "4env/env.h"
#include "4env/env_header.h"
#include "4env/env_local.h"

#include "utils.h"
#include "os.hpp"

using namespace upscaledb;

struct EnvFixture {
  uint32_t m_flags;

  EnvFixture(uint32_t flags = 0)
    : m_flags(flags) {
    os::unlink(Utils::opath(".test"));
  }

  void createCloseTest() {
    ups_env_t *env;

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_env_close(0, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void createCloseOpenCloseTest() {
    ups_env_t *env;

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));
    REQUIRE(0 == ups_env_close(env, 0));

    if (!(m_flags & UPS_IN_MEMORY)) {
      REQUIRE(0 == ups_env_open(&env, Utils::opath(".test"), 0, 0));
      REQUIRE(0 == ups_env_close(env, 0));
    }
  }

  void createCloseOpenCloseWithDatabasesTest() {
    ups_env_t *env;
    ups_db_t *db, *db2;

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_create_db(0, &db, 333, 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_create_db(env, 0, 333, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 333, 0, 0));
    REQUIRE(UPS_DATABASE_ALREADY_EXISTS ==
        ups_env_create_db(env, &db2, 333, 0, 0));
    REQUIRE(0 == ups_db_close(db, 0));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_open_db(0, &db, 333, 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_open_db(env, 0, 333, 0, 0));

    if (!(m_flags & UPS_IN_MEMORY)) {
      REQUIRE(0 == ups_env_open_db(env, &db, 333, 0, 0));
      REQUIRE(UPS_DATABASE_ALREADY_OPEN ==
          ups_env_open_db(env, &db, 333, 0, 0));
      REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

      REQUIRE(0 == ups_env_open(&env, Utils::opath(".test"), 0, 0));

      REQUIRE(0 == ups_env_open_db(env, &db, 333, 0, 0));
      REQUIRE(0 == ups_db_close(db, 0));
    }
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  /*
   * Create and ENV using EXtended parameters, than close it.
   *
   * Open the ENV again and check the parameters before and after
   * creating the first database.
   */
  void createCloseEmptyOpenCloseWithDatabasesTest() {
    ups_env_t *env;
    ups_db_t *db[128], *dbx;
    int i;
    const ups_parameter_t parameters[] = {
       { UPS_PARAM_CACHESIZE, 128 * 1024 },
       { UPS_PARAM_PAGESIZE, 64 * 1024 },
       { 0, 0 }
    };
    const ups_parameter_t parameters2[] = {
       { UPS_PARAM_CACHESIZE, 128 * 1024 },
       { 0, 0 }
    };

    ups_parameter_t ps[] = {
       { UPS_PARAM_CACHESIZE,0},
       { UPS_PARAM_PAGESIZE, 0},
       { UPS_PARAM_MAX_DATABASES, 0},
       { 0, 0 }
    };

    REQUIRE(0 == ups_env_create(&env, Utils::opath(".test"),
                        m_flags, 0664, parameters));
    REQUIRE(0 == ups_env_get_parameters(env, ps));
    REQUIRE((uint64_t)(128 * 1024u) == ps[0].value);
    REQUIRE((uint64_t)(64 * 1024u) == ps[1].value);
    REQUIRE((uint64_t)2334u == ps[2].value);

    /* close and re-open the ENV */
    if (!(m_flags & UPS_IN_MEMORY)) {
      REQUIRE(0 == ups_env_close(env, 0));

      REQUIRE(0 ==
        ups_env_open(&env, Utils::opath(".test"), m_flags, parameters2));
    }

    REQUIRE(0 == ups_env_get_parameters(env, ps));
    REQUIRE((uint64_t)(128 * 1024u) == ps[0].value);
    REQUIRE((uint64_t)(1024 * 64u) == ps[1].value);
    REQUIRE(2334ull == ps[2].value);

    /* now create 128 DBs; we said we would, anyway, when creating the
     * ENV ! */
    for (i = 0; i < 128; i++) {
      int j;

      REQUIRE(0 == ups_env_create_db(env, &db[i], i + 100, 0, 0));
      REQUIRE(UPS_DATABASE_ALREADY_EXISTS ==
          ups_env_create_db(env, &dbx, i + 100, 0, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
      REQUIRE(0 == ups_env_open_db(env, &db[i], i + 100, 0, 0));

      for (j = 0; ps[j].name; j++)
        ps[j].value = 0;
    }

    for (i = 0; i < 128; i++)
      REQUIRE(0 == ups_db_close(db[i], 0));

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void autoCleanupTest(void)
  {
    ups_env_t *env;
    ups_db_t *db[3];
    ups_cursor_t *c[5];

    REQUIRE(0 ==
      ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));
    for (int i = 0; i < 3; i++)
      REQUIRE(0 == ups_env_create_db(env, &db[i], i+1, 0, 0));
    for (int i = 0; i < 5; i++)
      REQUIRE(0 == ups_cursor_create(&c[i], db[0], 0, 0));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void autoCleanup2Test() {
    ups_env_t *env;
    ups_db_t *db;

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void readOnlyTest() {
    ups_db_t *db, *db2;
    ups_env_t *env;
    ups_key_t key;
    ups_record_t rec;
    ups_cursor_t *cursor;
    ::memset(&key, 0, sizeof(key));
    ::memset(&rec, 0, sizeof(rec));

    REQUIRE(0 == ups_env_create(&env, Utils::opath(".test"), 0, 0664, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 333, 0, 0));
    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, 0));

    REQUIRE(0 == ups_env_open(&env, Utils::opath(".test"), UPS_READ_ONLY, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 333, 0, 0));

    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    REQUIRE(UPS_DATABASE_ALREADY_OPEN ==
        ups_env_open_db(env, &db2, 333, 0, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_env_create_db(env, &db2, 444, 0, 0));

    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_db_erase(db, 0, &key, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_cursor_overwrite(cursor, &rec, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_cursor_insert(cursor, &key, &rec, 0));
    REQUIRE(UPS_WRITE_PROTECTED ==
        ups_cursor_erase(cursor, 0));

    REQUIRE(0 == ups_cursor_close(cursor));
    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void createPagesizeReopenTest() {
    ups_env_t *env;
    ups_parameter_t ps[] = { { UPS_PARAM_PAGESIZE, 1024 * 128 }, { 0, 0 } };

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0644, &ps[0]));
    if (!(m_flags & UPS_IN_MEMORY)) {
      REQUIRE(0 == ups_env_close(env, 0));
      REQUIRE(0 ==
          ups_env_open(&env, Utils::opath(".test"), m_flags, 0));
    }
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void openFailCloseTest() {
    ups_env_t *env;

    REQUIRE(UPS_FILE_NOT_FOUND ==
        ups_env_open(&env, "xxxxxx...", 0, 0));
    REQUIRE((ups_env_t *)0 == env);
  }

  void openWithKeysizeTest() {
    ups_env_t *env;

    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_open(0, Utils::opath(".test"), m_flags, 0));
    REQUIRE(UPS_FILE_NOT_FOUND ==
        ups_env_open(&env, Utils::opath(".test"), m_flags, 0));
  }

  void createDbWithKeysizeTest() {
    ups_env_t *env;
    ups_db_t *db;
    ups_parameter_t parameters2[] = {
       { UPS_PARAM_KEYSIZE, (uint64_t)64 },
       { 0, 0ull }
    };

    REQUIRE(0 == ups_env_create(&env, Utils::opath(".test"), m_flags, 0644, 0));

    REQUIRE(0 == ups_env_create_db(env, &db, 333, 0, parameters2));
    REQUIRE((uint16_t)64 == ((LocalDatabase *)db)->config().key_size);
    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  // check to make sure both create and open_ex support accessing more
  // than DB_MAX_INDICES DBs in one env:
  void createAndOpenMultiDbTest() {
#undef MAX
#define MAX 256
    ups_env_t *env;
    ups_db_t *db[MAX];
    int i;
    ups_key_t key;
    ups_record_t rec;
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

    if (m_flags & UPS_IN_MEMORY) {
      REQUIRE(UPS_INV_PARAMETER ==
        ups_env_create(&env, Utils::opath(".test"),
            m_flags, 0644, parameters2));
      parameters2[1].value = 0; // page_size := 0
    }
    else {
      REQUIRE(UPS_INV_PARAMETER ==
        ups_env_create(&env, Utils::opath(".test"),
          m_flags | UPS_CACHE_UNLIMITED, 0644, parameters2));
      parameters2[1].value = 65536; // page_size := 64K
    }

    if (m_flags & UPS_IN_MEMORY)
      parameters2[0].value = 0; // cache_size := 0

    REQUIRE(0 ==
      ups_env_create(&env, Utils::opath(".test"), m_flags, 0644, parameters2));

    char buffer[20] = {0};

    // create DBs
    for (i = 0; i < MAX; i++) {
      REQUIRE(0 ==
          ups_env_create_db(env, &db[i], i + 1, 0, parameters));
      memset(&key, 0, sizeof(key));
      memset(&rec, 0, sizeof(rec));
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      key.size = sizeof(buffer);
      rec.data = &buffer[0];
      rec.size = sizeof(buffer);
      REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
      if (!(m_flags & UPS_IN_MEMORY))
        REQUIRE(0 == ups_db_close(db[i], 0));
    }

    if (!(m_flags & UPS_IN_MEMORY)) {
      REQUIRE(0 == ups_env_close(env, 0));

      // open DBs
      // page_size param not allowed
      REQUIRE(UPS_INV_PARAMETER ==
        ups_env_open(&env, Utils::opath(".test"), m_flags, parameters2));
      REQUIRE(0 ==
        ups_env_open(&env, Utils::opath(".test"), m_flags, parameters3));
      // key_size param not allowed
      REQUIRE(UPS_INV_PARAMETER ==
        ups_env_open_db(env, &db[0], 1, 0, parameters));
    }

    for (i = 0; i < MAX; i++) {
      if (!(m_flags & UPS_IN_MEMORY)) {
        REQUIRE(0 ==
            ups_env_open_db(env, &db[i], i + 1, 0, 0));
      }
      memset(&key, 0, sizeof(key));
      memset(&rec, 0, sizeof(rec));
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      key.size = sizeof(buffer);
      REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
      REQUIRE(*(int *)key.data == i);
      REQUIRE(*(int *)rec.data == i);
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void multiDbTest() {
    int i;
    ups_env_t *env;
    ups_db_t *db[10];

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < 10; i++) {
      REQUIRE(0 == ups_env_create_db(env, &db[i],
            (uint16_t)i + 1, 0, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
      REQUIRE(0 == ups_env_open_db(env, &db[i],
            (uint16_t)i + 1, 0, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    for (i = 0; i < 10; i++) {
      REQUIRE(0 == ups_env_open_db(env, &db[i],
            (uint16_t)i + 1, 0, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void multiDbTest2() {
    int i;
    ups_env_t *env;
    ups_db_t *db[10];

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < 10; i++)
      REQUIRE(0 == ups_env_create_db(env, &db[i],
            (uint16_t)i + 1, 0, 0));

    for (i = 0; i < 10; i++)
      REQUIRE(0 == ups_db_close(db[i], 0));

    if (!(m_flags & UPS_IN_MEMORY)) {
      for (i = 0; i < 10; i++) {
        REQUIRE(0 == ups_env_open_db(env, &db[i],
              (uint16_t)i + 1, 0, 0));
        REQUIRE(0 == ups_db_close(db[i], 0));
      }
    }

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void multiDbInsertFindTest() {
    int i;
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ups_env_t *env;
    ups_db_t *db[MAX_DB];
    ups_record_t rec;
    ups_key_t key;

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(env, &db[i],
            (uint16_t)i + 1, 0, 0));

      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        key.data = &value;
        key.size = sizeof(value);
        rec.data = &value;
        rec.size = sizeof(value);

        REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
      }
    }

    for (i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        key.data = (void *)&value;
        key.size = sizeof(value);

        REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
        REQUIRE(value == *(int *)key.data);
        REQUIRE((uint16_t)sizeof(value) == key.size);
      }
    }

    if (!(m_flags & UPS_IN_MEMORY)) {
      for (i = 0; i < MAX_DB; i++) {
        REQUIRE(0 == ups_db_close(db[i], 0));
        REQUIRE(0 == ups_env_open_db(env, &db[i],
              (uint16_t)i + 1, 0, 0));
        for (int j = 0; j < MAX_ITEMS; j++) {
          int value = j * (i + 1);
          memset(&key, 0, sizeof(key));
          memset(&rec, 0, sizeof(rec));
          key.data = (void *)&value;
          key.size = sizeof(value);

          REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
          REQUIRE(value == *(int *)key.data);
          REQUIRE((uint16_t)sizeof(value) == key.size);
        }
      }
    }

    for (i = 0; i < MAX_DB; i++)
      REQUIRE(0 == ups_db_close(db[i], 0));

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void multiDbInsertFindExtendedTest() {
    int i;
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ups_env_t *env;
    ups_db_t *db[MAX_DB];
    ups_record_t rec;
    ups_key_t key;
    char buffer[512];

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(env, &db[i], (uint16_t)i + 1, 0, 0));

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

        REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
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

        REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
        REQUIRE((uint32_t)sizeof(buffer) == rec.size);
        REQUIRE(0 == memcmp(buffer, rec.data, rec.size));
      }
    }

    if (!(m_flags & UPS_IN_MEMORY)) {
      for (i = 0; i < MAX_DB; i++) {
        REQUIRE(0 == ups_db_close(db[i], 0));
        REQUIRE(0 == ups_env_open_db(env, &db[i],
              (uint16_t)i + 1, 0, 0));
        for (int j = 0; j < MAX_ITEMS; j++) {
          int value = j * (i + 1);
          memset(&key, 0, sizeof(key));
          memset(&rec, 0, sizeof(rec));
          memset(buffer, (char)value, sizeof(buffer));
          key.data = buffer;
          key.size = sizeof(buffer);
          sprintf(buffer, "%08x%08x", j, i + 1);

          REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
          REQUIRE((uint32_t)sizeof(buffer) == rec.size);
          REQUIRE(0 == memcmp(buffer, rec.data, rec.size));
        }
      }
    }

    for (i = 0; i < MAX_DB; i++)
      REQUIRE(0 == ups_db_close(db[i], 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void multiDbInsertFindExtendedEraseTest() {
    int i;
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ups_env_t *env;
    ups_db_t *db[MAX_DB];
    ups_record_t rec;
    ups_key_t key;
    char buffer[512];

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(env, &db[i], (uint16_t)i + 1, 0, 0));

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

        REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
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

        REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
        REQUIRE((uint32_t)sizeof(buffer) == rec.size);
        REQUIRE(0 == memcmp(buffer, rec.data, rec.size));
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

        REQUIRE(0 == ups_db_erase(db[i], 0, &key, 0));
      }
    }

    if (!(m_flags & UPS_IN_MEMORY)) {
      for (i = 0; i < MAX_DB; i++) {
        REQUIRE(0 == ups_db_close(db[i], 0));
        REQUIRE(0 == ups_env_open_db(env, &db[i],
              (uint16_t)i + 1, 0, 0));
        for (int j = 0; j < MAX_ITEMS; j++) {
          int value = j * (i + 1);
          memset(&key, 0, sizeof(key));
          memset(&rec, 0, sizeof(rec));
          memset(buffer, (char)value, sizeof(buffer));
          key.data = buffer;
          key.size = sizeof(buffer);
          sprintf(buffer, "%08x%08x", j, i+1);

          if (j & 1) { // must exist
            REQUIRE(0 ==
                ups_db_find(db[i], 0, &key, &rec, 0));
            REQUIRE((uint32_t)sizeof(buffer) == rec.size);
            REQUIRE(0 == memcmp(buffer, rec.data, rec.size));
          }
          else { // was deleted
            REQUIRE(UPS_KEY_NOT_FOUND ==
                ups_db_find(db[i], 0, &key, &rec, 0));
          }
        }
      }
    }

    for (i = 0; i < MAX_DB; i++)
      REQUIRE(0 == ups_db_close(db[i], 0));

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void multiDbInsertCursorTest() {
    int i;
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ups_env_t *env;
    ups_db_t *db[MAX_DB];
    ups_cursor_t *cursor[MAX_DB];
    ups_record_t rec;
    ups_key_t key;
    char buffer[512];

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(env, &db[i],
            (uint16_t)i + 1, 0, 0));
      REQUIRE(0 == ups_cursor_create(&cursor[i], db[i], 0, 0));

      for (int j = 0; j < MAX_ITEMS; j++) {
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        sprintf(buffer, "%08x%08x", j, i+1);
        key.data = buffer;
        key.size = (uint16_t)strlen(buffer) + 1;
        rec.data = buffer;
        rec.size = (uint16_t)strlen(buffer) + 1;

        REQUIRE(0 == ups_cursor_insert(cursor[i], &key, &rec, 0));
      }
    }

    for (i = 0; i < MAX_DB; i++) {
      memset(&key, 0, sizeof(key));
      memset(&rec, 0, sizeof(rec));

      REQUIRE(0 == ups_cursor_move(cursor[i], &key, &rec, UPS_CURSOR_FIRST));
      sprintf(buffer, "%08x%08x", 0, i+1);
      REQUIRE((uint32_t)(strlen(buffer) + 1) == rec.size);
      REQUIRE(0 == strcmp(buffer, (char *)rec.data));

      for (int j = 1; j < MAX_ITEMS; j++) {
        REQUIRE(0 == ups_cursor_move(cursor[i], &key, &rec, UPS_CURSOR_NEXT));
        sprintf(buffer, "%08x%08x", j, i+1);
        REQUIRE((uint32_t)(strlen(buffer) + 1) == rec.size);
        REQUIRE(0 == strcmp(buffer, (char *)rec.data));
      }
    }

    for (i = 0; i < MAX_DB; i++) {
      for (int j = 0; j < MAX_ITEMS; j += 2) { // delete every 2nd entry
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        sprintf(buffer, "%08x%08x", j, i + 1);
        key.data = buffer;
        key.size = (uint16_t)strlen(buffer) + 1;

        REQUIRE(0 == ups_cursor_find(cursor[i], &key, 0, 0));
        REQUIRE(0 == ups_cursor_erase(cursor[i], 0));
      }
    }

    if (!(m_flags & UPS_IN_MEMORY)) {
      for (i = 0; i < MAX_DB; i++) {
        REQUIRE(0 == ups_cursor_close(cursor[i]));
        REQUIRE(0 == ups_db_close(db[i], 0));
        REQUIRE(0 == ups_env_open_db(env, &db[i],
              (uint16_t)i + 1, 0, 0));
        REQUIRE(0 == ups_cursor_create(&cursor[i], db[i], 0, 0));
        for (int j = 0; j < MAX_ITEMS; j++) {
          memset(&key, 0, sizeof(key));
          memset(&rec, 0, sizeof(rec));
          sprintf(buffer, "%08x%08x", j, i + 1);
          key.data = buffer;
          key.size = (uint16_t)strlen(buffer) + 1;

          if (j & 1) { // must exist
            REQUIRE(0 ==
                ups_cursor_find(cursor[i], &key, 0, 0));
            REQUIRE(0 ==
                ups_cursor_move(cursor[i], 0, &rec, 0));
            REQUIRE((uint32_t)(strlen(buffer) + 1) == rec.size);
            REQUIRE(0 == strcmp(buffer, (char *)rec.data));
          }
          else { // was deleted
            REQUIRE(UPS_KEY_NOT_FOUND ==
                ups_cursor_find(cursor[i], &key, 0, 0));
          }
        }
      }
    }

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_cursor_close(cursor[i]));
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void multiDbInsertFindExtendedCloseReopenTest() {
    int i;
    const int MAX_DB = 5;
    const int MAX_ITEMS = 300;
    ups_env_t *env;
    ups_db_t *db[MAX_DB];
    ups_record_t rec;
    ups_key_t key;
    char buffer[512];

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++)
      REQUIRE(0 == ups_env_create_db(env, &db[i], (uint16_t)i + 1, 0, 0));

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

        REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
      }
      if (!(m_flags & UPS_IN_MEMORY))
        REQUIRE(0 == ups_db_close(db[i], 0));
    }

    if (!(m_flags & UPS_IN_MEMORY)) {
      REQUIRE(0 == ups_env_close(env, 0));
      REQUIRE(0 == ups_env_open(&env, Utils::opath(".test"), m_flags, 0));
    }

    for (i = 0; i < MAX_DB; i++) {
      if (!(m_flags & UPS_IN_MEMORY)) {
        REQUIRE(0 == ups_env_open_db(env, &db[i],
              (uint16_t)i + 1, 0, 0));
      }
      for (int j = 0; j < MAX_ITEMS; j++) {
        int value = j * (i + 1);
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        memset(buffer, (char)value, sizeof(buffer));
        key.data = buffer;
        key.size = sizeof(buffer);
        sprintf(buffer, "%08x%08x", j, i+1);

		REQUIRE(0 == ups_db_find(db[i], 0, &key, &rec, 0));
        REQUIRE((uint32_t)sizeof(buffer) == rec.size);
        REQUIRE(0 == memcmp(buffer, rec.data, rec.size));
      }
    }

    for (i = 0; i < MAX_DB; i++)
      REQUIRE(0 == ups_db_close(db[i], 0));

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void renameOpenDatabases() {
    int i;
    const int MAX_DB = 10;
    ups_env_t *env;
    ups_db_t *db[MAX_DB];

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++)
      REQUIRE(0 == ups_env_create_db(env, &db[i],
            (uint16_t)i+1, 0, 0));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_rename_db(0, 1, 2, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_rename_db(env, 0, 2, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_rename_db(env, 1, 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_rename_db(env, 1, 0xffff, 0));
    REQUIRE(0 ==
        ups_env_rename_db(env, 1, 1, 0));
    REQUIRE(UPS_DATABASE_ALREADY_EXISTS ==
        ups_env_rename_db(env, 1, 5, 0));
    REQUIRE(UPS_DATABASE_NOT_FOUND ==
        ups_env_rename_db(env, 1000, 20, 0));

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_rename_db(env,
            (uint16_t)i + 1, (uint16_t)i + 1000, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    if (!(m_flags & UPS_IN_MEMORY)) {
      for (i = 0; i < MAX_DB; i++) {
        REQUIRE(0 == ups_env_open_db(env, &db[i],
              (uint16_t)i+1000, 0, 0));
      }

      for (i = 0; i < MAX_DB; i++)
        REQUIRE(0 == ups_db_close(db[i], 0));
    }

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void renameClosedDatabases() {
    int i;
    const int MAX_DB = 10;
    ups_env_t *env;
    ups_db_t *db[MAX_DB];

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(env, &db[i],
            (uint16_t)i + 1, 0, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_rename_db(env,
            (uint16_t)i + 1, (uint16_t)i + 1000, 0));
    }

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_open_db(env, &db[i],
            (uint16_t)i + 1000, 0, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void eraseOpenDatabases() {
    int i;
    const int MAX_DB = 1;
    ups_env_t *env;
    ups_db_t *db[MAX_DB];

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(env, &db[i],
            (uint16_t)i + 1, 0, 0));
    }

    REQUIRE(UPS_INV_PARAMETER ==
            ups_env_erase_db(0, (uint16_t)i + 1, 0));
    REQUIRE(UPS_INV_PARAMETER ==
            ups_env_erase_db(env, 0, 0));

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(UPS_DATABASE_ALREADY_OPEN ==
              ups_env_erase_db(env, (uint16_t)i + 1, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
      if (m_flags & UPS_IN_MEMORY) {
        REQUIRE(UPS_DATABASE_NOT_FOUND ==
            ups_env_erase_db(env, (uint16_t)i + 1, 0));
      }
      else {
        REQUIRE(0 ==
            ups_env_erase_db(env, (uint16_t)i + 1, 0));
      }
    }

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void eraseUnknownDatabases() {
    int i;
    const int MAX_DB = 1;
    ups_env_t *env;
    ups_db_t *db[MAX_DB];

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_create_db(env, &db[i],
            (uint16_t)i + 1, 0, 0));
    }

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(UPS_DATABASE_NOT_FOUND ==
              ups_env_erase_db(env, (uint16_t)i + 1000, 0));
      REQUIRE(0 == ups_db_close(db[i], 0));
      REQUIRE(UPS_DATABASE_NOT_FOUND ==
              ups_env_erase_db(env, (uint16_t)i + 1000, 0));
    }

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void eraseMultipleDatabases() {
    int i, j;
    const int MAX_DB = 13;
    const int MAX_ITEMS = 300;
    ups_env_t *env;
    ups_db_t *db[MAX_DB];
    ups_record_t rec;
    ups_key_t key;
    char buffer[512];
    ups_parameter_t ps[] = {
      { UPS_PARAM_PAGESIZE, 1024 * 6 },
      { 0, 0 }
    };
    ups_parameter_t ps2[] = {
      { UPS_PARAM_KEYSIZE, sizeof(buffer) },
      { 0, 0 }
    };

    REQUIRE(0 ==
      ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, ps));

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 ==
        ups_env_create_db(env, &db[i], (uint16_t)i + 1, 0, ps2));
      for (j = 0; j < MAX_ITEMS; j++) {
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        memset(buffer, 0, sizeof(buffer));
        sprintf(buffer, "%08x%08x", j, i+1);
        key.data = buffer;
        key.size = sizeof(buffer);
        key.flags = UPS_KEY_USER_ALLOC;
        rec.data = buffer;
        rec.size = sizeof(buffer);
        rec.flags = UPS_RECORD_USER_ALLOC;

        REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
      }
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(((m_flags & UPS_IN_MEMORY)
                ? UPS_DATABASE_NOT_FOUND
                : 0) ==
        ups_env_erase_db(env, (uint16_t)i + 1, 0));
    }

    for (i = 0; i < 10; i++) {
      REQUIRE(((m_flags & UPS_IN_MEMORY)
                ? UPS_INV_PARAMETER
                : UPS_DATABASE_NOT_FOUND) ==
        ups_env_open_db(env, &db[i], (uint16_t)i + 1, 0, 0));
    }

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void eraseMultipleDatabasesReopenEnv() {
    int i, j;
    const int MAX_DB = 13;
    const int MAX_ITEMS = 300;
    ups_env_t *env;
    ups_db_t *db[MAX_DB];
    ups_record_t rec;
    ups_key_t key;
    char buffer[512];

    REQUIRE(0 ==
      ups_env_create(&env, Utils::opath(".test"),
              m_flags | UPS_DISABLE_RECLAIM_INTERNAL, 0664, 0));

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 ==
        ups_env_create_db(env, &db[i], (uint16_t)i + 1, 0, 0));
      for (j = 0; j < MAX_ITEMS; j++) {
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        memset(buffer, 0, sizeof(buffer));
        sprintf(buffer, "%08x%08x", j, i+1);
        key.data = buffer;
        key.size = sizeof(buffer);
        key.flags = UPS_KEY_USER_ALLOC;
        rec.data = buffer;
        rec.size = sizeof(buffer);
        rec.flags = UPS_RECORD_USER_ALLOC;

        REQUIRE(0 == ups_db_insert(db[i], 0, &key, &rec, 0));
      }
      REQUIRE(0 == ups_db_close(db[i], 0));
    }

    REQUIRE(0 == ups_env_close(env, 0));
    REQUIRE(0 == ups_env_open(&env, Utils::opath(".test"), m_flags, 0));

    for (i = 0; i < MAX_DB; i++) {
      REQUIRE(0 == ups_env_erase_db(env, (uint16_t)i + 1, 0));
    }

    for (i = 0; i < 10; i++) {
      REQUIRE(UPS_DATABASE_NOT_FOUND ==
        ups_env_open_db(env, &db[i], (uint16_t)i + 1, 0, 0));
    }

    REQUIRE(0 == ups_env_close(env, 0));
  }

  void limitsReachedTest() {
    int i;
    const int MAX_DB = 579 + 1;
    ups_env_t *env;
    ups_db_t *db[MAX_DB];

    REQUIRE(0 == ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < MAX_DB - 1; i++)
      REQUIRE(0 == ups_env_create_db(env, &db[i], (uint16_t)i + 1, 0, 0));

    REQUIRE(UPS_LIMITS_REACHED ==
        ups_env_create_db(env, &db[i], (uint16_t)i + 1, 0, 0));

    for (i = 0; i < MAX_DB - 1; i++)
      REQUIRE(0 == ups_db_close(db[i], 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void getDatabaseNamesTest() {
    ups_env_t *env;
    ups_db_t *db1, *db2, *db3;
    uint16_t names[5];
    uint32_t names_size = 0;

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_get_database_names(0, names, &names_size));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_get_database_names(env, 0, &names_size));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_env_get_database_names(env, names, 0));

    names_size = 1;
    REQUIRE(0 ==
        ups_env_get_database_names(env, names, &names_size));
    REQUIRE((uint32_t)0 == names_size);

    REQUIRE(0 ==
        ups_env_create_db(env, &db1, 111, 0, 0));
    names_size = 0;
    REQUIRE(UPS_LIMITS_REACHED ==
        ups_env_get_database_names(env, names, &names_size));

    names_size = 1;
    REQUIRE(0 ==
        ups_env_get_database_names(env, names, &names_size));
    REQUIRE((uint32_t)1 == names_size);
    REQUIRE((uint16_t)111 == names[0]);

    REQUIRE(0 ==
        ups_env_create_db(env, &db2, 222, 0, 0));
    names_size = 1;
    REQUIRE(UPS_LIMITS_REACHED ==
        ups_env_get_database_names(env, names, &names_size));

    REQUIRE(0 ==
        ups_env_create_db(env, &db3, 333, 0, 0));
    names_size = 5;
    REQUIRE(0 ==
        ups_env_get_database_names(env, names, &names_size));
    REQUIRE((uint32_t)3 == names_size);
    REQUIRE((uint16_t)111 == names[0]);
    REQUIRE((uint16_t)222 == names[1]);
    REQUIRE((uint16_t)333 == names[2]);

    REQUIRE(0 == ups_db_close(db2, 0));
    if (!(m_flags & UPS_IN_MEMORY)) {
      REQUIRE(0 == ups_env_erase_db(env, 222, 0));
      names_size = 5;
      REQUIRE(0 ==
          ups_env_get_database_names(env, names, &names_size));
      REQUIRE((uint32_t)2 == names_size);
      REQUIRE((uint16_t)111 == names[0]);
      REQUIRE((uint16_t)333 == names[1]);
    }

    REQUIRE(0 == ups_db_close(db1, 0));
    REQUIRE(0 == ups_db_close(db3, 0));
    REQUIRE(0 == ups_env_close(env, 0));
  }

  void createOpenEmptyTest() {
    ups_env_t *env;
    ups_db_t *db[10];

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));
    for (int i = 0; i < 10; i++)
      REQUIRE(0 ==
          ups_env_create_db(env, &db[i], 333+i, 0, 0));
    if (!(m_flags & UPS_IN_MEMORY)) {
      REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

      REQUIRE(0 ==
          ups_env_open(&env, Utils::opath(".test"), m_flags, 0));
      for (int i = 0; i < 10; i++) {
        REQUIRE(0 ==
          ups_env_open_db(env, &db[i], 333+i, 0, 0));
      }
    }
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void memoryDbTest() {
    int i;
    ups_env_t *env;
    ups_db_t *db[10];

    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test"), m_flags, 0664, 0));

    for (i = 0; i < 10; i++)
      REQUIRE(0 == ups_env_create_db(env, &db[i],
            (uint16_t)i + 1, 0, 0));

    for (i = 0; i < 10; i++)
      REQUIRE(0 == ups_db_close(db[i], 0));

    REQUIRE(0 == ups_env_close(env, 0));
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


TEST_CASE("Env-inmem/createCloseTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createCloseTest();
}

TEST_CASE("Env-inmem/createCloseOpenCloseTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createCloseOpenCloseTest();
}

TEST_CASE("Env-inmem/createCloseOpenCloseWithDatabasesTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createCloseOpenCloseWithDatabasesTest();
}

TEST_CASE("Env-inmem/createPagesizeReopenTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createPagesizeReopenTest();
}

TEST_CASE("Env-inmem/createDbWithKeysizeTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createDbWithKeysizeTest();
}

TEST_CASE("Env-inmem/createAndOpenMultiDbTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createAndOpenMultiDbTest();
}

TEST_CASE("Env-inmem/autoCleanupTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.autoCleanupTest();
}

TEST_CASE("Env-inmem/autoCleanup2Test", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.autoCleanup2Test();
}

TEST_CASE("Env-inmem/memoryDbTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.memoryDbTest();
}

TEST_CASE("Env-inmem/multiDbTest2", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.multiDbTest2();
}

TEST_CASE("Env-inmem/multiDbInsertFindTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.multiDbInsertFindTest();
}

TEST_CASE("Env-inmem/multiDbInsertFindExtendedTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.multiDbInsertFindExtendedTest();
}

TEST_CASE("Env-inmem/multiDbInsertFindExtendedEraseTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.multiDbInsertFindExtendedEraseTest();
}

TEST_CASE("Env-inmem/multiDbInsertCursorTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.multiDbInsertCursorTest();
}

TEST_CASE("Env-inmem/multiDbInsertFindExtendedCloseReopenTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.multiDbInsertFindExtendedCloseReopenTest();
}

TEST_CASE("Env-inmem/renameOpenDatabases", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.renameOpenDatabases();
}

TEST_CASE("Env-inmem/eraseOpenDatabases", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.eraseOpenDatabases();
}

TEST_CASE("Env-inmem/eraseUnknownDatabases", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.eraseUnknownDatabases();
}

TEST_CASE("Env-inmem/limitsReachedTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.limitsReachedTest();
}

TEST_CASE("Env-inmem/getDatabaseNamesTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.getDatabaseNamesTest();
}

TEST_CASE("Env-inmem/createOpenEmptyTest", "")
{
  EnvFixture f(UPS_IN_MEMORY);
  f.createOpenEmptyTest();
}

