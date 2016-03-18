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

#include <string.h>
#include <assert.h>

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "4env/env_local.h"

using namespace upscaledb;

TEST_CASE("Aes/disabledIfInMemory", "")
{
  ups_env_t *env;
  ups_parameter_t p[] = {
          { UPS_PARAM_ENCRYPTION_KEY, (uint64_t)"foo" },
          { 0, 0 }
  };

  REQUIRE(UPS_INV_PARAMETER ==
          ups_env_create(&env, Utils::opath("test.db"), 
                  UPS_IN_MEMORY, 0644, p));
}

TEST_CASE("Aes/disableMmap", "")
{
  ups_env_t *env;
  ups_parameter_t p[] = {
          { UPS_PARAM_ENCRYPTION_KEY, (uint64_t)"foo" },
          { 0, 0 }
  };
  ups_parameter_t bad[] = {
          { UPS_PARAM_ENCRYPTION_KEY, (uint64_t)"bar" },
          { 0, 0 }
  };

  REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0644, p));
  REQUIRE((((Env *)env)->flags() & UPS_DISABLE_MMAP) != 0);
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

  REQUIRE(UPS_INV_FILE_HEADER ==
                  ups_env_open(&env, Utils::opath("test.db"), 0, 0));
  REQUIRE(UPS_INV_FILE_HEADER ==
                  ups_env_open(&env, Utils::opath("test.db"), 0, bad));
  REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, p));
  REQUIRE((((Env *)env)->flags() & UPS_DISABLE_MMAP) != 0);
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}

TEST_CASE("Aes/simpleInsert", "")
{
  ups_env_t *env;
  ups_db_t *db;
  ups_parameter_t p[] = {
          { UPS_PARAM_ENCRYPTION_KEY, (uint64_t)"foo" },
          { 0, 0 }
  };

  REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0644, p));
  REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

  char buffer[512];
  ups_key_t key = {0};
  ups_record_t rec = {0};
  for (int i = 0; i < 512; i++) {
    key.data = &i;
    key.size = sizeof(i);
    rec.data = buffer;
    rec.size = i;
    for (int j = 0; j < i; j++)
      buffer[j] = (char)j;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
  }

  for (int i = 0; i < 512; i++) {
    key.data = &i;
    key.size = sizeof(i);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE((uint16_t)i == rec.size);
    REQUIRE((uint16_t)sizeof(i) == key.size);
    REQUIRE(i == *(int *)key.data);
    for (int j = 0; j < i; j++)
      REQUIRE((char)j == ((char *)rec.data)[j]);
  }

  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

  // reopen and check again
  REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, p));
  REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));

  for (int i = 0; i < 512; i++) {
    key.data = &i;
    key.size = sizeof(i);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE((uint16_t)i == rec.size);
    REQUIRE((uint16_t)sizeof(i) == key.size);
    REQUIRE(i == *(int *)key.data);
    for (int j = 0; j < i; j++)
      REQUIRE((char)j == ((char *)rec.data)[j]);
  }

  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}

TEST_CASE("Aes/transactionInsert", "")
{
  ups_env_t *env;
  ups_db_t *db;
  ups_parameter_t p[] = {
          { UPS_PARAM_ENCRYPTION_KEY, (uint64_t)"foo" },
          { 0, 0 }
  };

  REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"),
                          UPS_ENABLE_TRANSACTIONS, 0644, p));
  REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

  char buffer[512];
  ups_key_t key = {0};
  ups_record_t rec = {0};
  for (int i = 0; i < 512; i++) {
    key.data = &i;
    key.size = sizeof(i);
    rec.data = buffer;
    rec.size = i;
    for (int j = 0; j < i; j++)
      buffer[j] = (char)j;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
  }

  for (int i = 0; i < 512; i++) {
    key.data = &i;
    key.size = sizeof(i);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE((uint16_t)i == rec.size);
    REQUIRE((uint16_t)sizeof(i) == key.size);
    REQUIRE(i == *(int *)key.data);
    for (int j = 0; j < i; j++)
      REQUIRE((char)j == ((char *)rec.data)[j]);
  }

  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG));

  // reopen and check again
  REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"),
                          UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY, p));
  REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));

  for (int i = 0; i < 512; i++) {
    key.data = &i;
    key.size = sizeof(i);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE((uint16_t)i == rec.size);
    REQUIRE((uint16_t)sizeof(i) == key.size);
    REQUIRE(i == *(int *)key.data);
    for (int j = 0; j < i; j++)
      REQUIRE((char)j == ((char *)rec.data)[j]);
  }

  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}

