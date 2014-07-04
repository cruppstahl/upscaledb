/**
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include <string.h>
#include <assert.h>

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "../src/env.h"
#include "../src/os.h"

using namespace hamsterdb;

TEST_CASE("Crc32/disabledIfInMemory", "")
{
  ham_env_t *env;

  REQUIRE(HAM_INV_PARAMETER ==
          ham_env_create(&env, Utils::opath("test.db"), 
                  HAM_IN_MEMORY | HAM_ENABLE_CRC32, 0644, 0));
}

TEST_CASE("Crc32/notPersistentFlag", "")
{
  Environment *e;
  ham_env_t *env;

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 
                  HAM_ENABLE_CRC32, 0644, 0));
  e = (Environment *)env;
  REQUIRE((e->get_flags() & HAM_ENABLE_CRC32) != 0);
  REQUIRE(0 == ham_env_close(env, 0));

  REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"), 0, 0));
  e = (Environment *)env;
  REQUIRE((e->get_flags() & HAM_ENABLE_CRC32) == 0);
  REQUIRE(0 == ham_env_close(env, 0));

  REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"),
                  HAM_ENABLE_CRC32, 0));
  e = (Environment *)env;
  REQUIRE((e->get_flags() & HAM_ENABLE_CRC32) != 0);
  REQUIRE(0 == ham_env_close(env, 0));
}

TEST_CASE("Crc32/corruptPageTest", "")
{
  ham_env_t *env;
  ham_db_t *db;

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 
                  HAM_ENABLE_CRC32, 0644, 0));
  REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, 0));
  ham_key_t key = {0};
  key.data = (void *)"1";
  key.size = 1;
  ham_record_t rec = {0};
  REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

  // flip a few bytes in page 16 * 1024
  File f;
  f.open(Utils::opath("test.db"), 0);
  f.pwrite(1024 * 16 + 200, "xxx", 3);
  f.close();

  REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"),
                  HAM_ENABLE_CRC32, 0));
  REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));
  memset(&rec, 0, sizeof(rec));
  REQUIRE(HAM_INTEGRITY_VIOLATED == ham_db_find(db, 0, &key, &rec, 0));
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}

TEST_CASE("Crc32/multipageBlobTest", "")
{
  ham_env_t *env;
  ham_db_t *db;
  char *buffer = (char *)::malloc(1024 * 32);
  memset(buffer, 0, 1024 * 32);

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 
                  HAM_ENABLE_CRC32, 0644, 0));
  REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, 0));
  ham_key_t key = {0};
  key.data = (void *)"1";
  key.size = 1;
  ham_record_t rec = {0};
  rec.data = buffer;
  rec.size = 1024 * 32;
  REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));

  ham_record_t rec2 = {0};
  REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

  // reopen, check
  REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"),
                  HAM_ENABLE_CRC32, 0));
  REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));
  memset(&rec, 0, sizeof(rec));
  REQUIRE(0 == ham_db_find(db, 0, &key, &rec, 0));

  // overwrite
  memset(rec.data, 1, 1024);
  REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, HAM_OVERWRITE));
  memset(&rec2, 0, sizeof(rec2));
  REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

  // reopen, check once more
  REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"),
                  HAM_ENABLE_CRC32, 0));
  REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));
  memset(&rec, 0, sizeof(rec));
  REQUIRE(0 == ham_db_find(db, 0, &key, &rec, 0));
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

  free(buffer);
}

TEST_CASE("Crc32/corruptMultipageBlobTest", "")
{
  ham_env_t *env;
  ham_db_t *db;
  char *buffer = (char *)::malloc(1024 * 32);
  memset(buffer, 0, 1024 * 32);

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 
                  HAM_ENABLE_CRC32, 0644, 0));
  REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, 0));
  ham_key_t key = {0};
  key.data = (void *)"1";
  key.size = 1;
  ham_record_t rec = {0};
  rec.data = buffer;
  rec.size = 1024 * 32;
  REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));

  ham_record_t rec2 = {0};
  REQUIRE(0 == ham_db_find(db, 0, &key, &rec2, 0));
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

  // flip a few bytes in page 32 * 1024
  File f;
  f.open(Utils::opath("test.db"), 0);
  f.pwrite(1024 * 32 + 200, "xxx", 3);
  f.close();

  // reopen, check
  REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"),
                  HAM_ENABLE_CRC32, 0));
  REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));
  memset(&rec, 0, sizeof(rec));
  REQUIRE(HAM_INTEGRITY_VIOLATED == ham_db_find(db, 0, &key, &rec, 0));
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

  free(buffer);
}

