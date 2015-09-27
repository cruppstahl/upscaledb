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

#include <string.h>
#include <assert.h>

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "1os/file.h"
#include "4env/env.h"

using namespace upscaledb;

TEST_CASE("Crc32/disabledIfInMemory", "")
{
  ups_env_t *env;

  REQUIRE(UPS_INV_PARAMETER ==
          ups_env_create(&env, Utils::opath("test.db"), 
                  UPS_IN_MEMORY | UPS_ENABLE_CRC32, 0644, 0));
}

TEST_CASE("Crc32/notPersistentFlag", "")
{
  Environment *e;
  ups_env_t *env;

  REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 
                  UPS_ENABLE_CRC32, 0644, 0));
  e = (Environment *)env;
  REQUIRE((e->get_flags() & UPS_ENABLE_CRC32) != 0);
  REQUIRE(0 == ups_env_close(env, 0));

  REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, 0));
  e = (Environment *)env;
  REQUIRE((e->get_flags() & UPS_ENABLE_CRC32) == 0);
  REQUIRE(0 == ups_env_close(env, 0));

  REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"),
                  UPS_ENABLE_CRC32, 0));
  e = (Environment *)env;
  REQUIRE((e->get_flags() & UPS_ENABLE_CRC32) != 0);
  REQUIRE(0 == ups_env_close(env, 0));
}

TEST_CASE("Crc32/corruptPageTest", "")
{
  ups_env_t *env;
  ups_db_t *db;

  REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 
                  UPS_ENABLE_CRC32, 0644, 0));
  REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));
  ups_key_t key = {0};
  key.data = (void *)"1";
  key.size = 1;
  ups_record_t rec = {0};
  REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

  // flip a few bytes in page 16 * 1024
  File f;
  f.open(Utils::opath("test.db"), 0);
  f.pwrite(1024 * 16 + 200, "xxx", 3);
  f.close();

  REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"),
                  UPS_ENABLE_CRC32, 0));
  REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));
  memset(&rec, 0, sizeof(rec));
  REQUIRE(UPS_INTEGRITY_VIOLATED == ups_db_find(db, 0, &key, &rec, 0));
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}

TEST_CASE("Crc32/multipageBlobTest", "")
{
  ups_env_t *env;
  ups_db_t *db;
  char *buffer = (char *)::malloc(1024 * 32);
  memset(buffer, 0, 1024 * 32);

  REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 
                  UPS_ENABLE_CRC32, 0644, 0));
  REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));
  ups_key_t key = {0};
  key.data = (void *)"1";
  key.size = 1;
  ups_record_t rec = {0};
  rec.data = buffer;
  rec.size = 1024 * 32;
  REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

  ups_record_t rec2 = {0};
  REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

  // reopen, check
  REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"),
                  UPS_ENABLE_CRC32, 0));
  REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));
  memset(&rec, 0, sizeof(rec));
  REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));

  // overwrite
  memset(rec.data, 1, 1024);
  REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
  memset(&rec2, 0, sizeof(rec2));
  REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

  // reopen, check once more
  REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"),
                  UPS_ENABLE_CRC32, 0));
  REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));
  memset(&rec, 0, sizeof(rec));
  REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

  free(buffer);
}

TEST_CASE("Crc32/corruptMultipageBlobTest", "")
{
  ups_env_t *env;
  ups_db_t *db;
  char *buffer = (char *)::malloc(1024 * 32);
  memset(buffer, 0, 1024 * 32);

  REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 
                  UPS_ENABLE_CRC32, 0644, 0));
  REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));
  ups_key_t key = {0};
  key.data = (void *)"1";
  key.size = 1;
  ups_record_t rec = {0};
  rec.data = buffer;
  rec.size = 1024 * 32;
  REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

  ups_record_t rec2 = {0};
  REQUIRE(0 == ups_db_find(db, 0, &key, &rec2, 0));
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

  // flip a few bytes in page 32 * 1024
  File f;
  f.open(Utils::opath("test.db"), 0);
  f.pwrite(1024 * 32 + 200, "xxx", 3);
  f.close();

  // reopen, check
  REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"),
                  UPS_ENABLE_CRC32, 0));
  REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));
  memset(&rec, 0, sizeof(rec));
  REQUIRE(UPS_INTEGRITY_VIOLATED == ups_db_find(db, 0, &key, &rec, 0));
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

  free(buffer);
}

