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

#include "4env/env_local.h"

using namespace hamsterdb;

TEST_CASE("Aes/disabledIfInMemory", "")
{
  ham_env_t *env;
  ham_parameter_t p[] = {
          { HAM_PARAM_ENCRYPTION_KEY, (uint64_t)"foo" },
          { 0, 0 }
  };

  REQUIRE(HAM_INV_PARAMETER ==
          ham_env_create(&env, Utils::opath("test.db"), 
                  HAM_IN_MEMORY, 0644, p));
}

TEST_CASE("Aes/disableMmap", "")
{
  ham_env_t *env;
  ham_parameter_t p[] = {
          { HAM_PARAM_ENCRYPTION_KEY, (uint64_t)"foo" },
          { 0, 0 }
  };
  ham_parameter_t bad[] = {
          { HAM_PARAM_ENCRYPTION_KEY, (uint64_t)"bar" },
          { 0, 0 }
  };

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0644, p));
  REQUIRE((((Environment *)env)->get_flags() & HAM_DISABLE_MMAP) != 0);
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

  REQUIRE(HAM_INV_FILE_HEADER ==
                  ham_env_open(&env, Utils::opath("test.db"), 0, 0));
  REQUIRE(HAM_INV_FILE_HEADER ==
                  ham_env_open(&env, Utils::opath("test.db"), 0, bad));
  REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"), 0, p));
  REQUIRE((((Environment *)env)->get_flags() & HAM_DISABLE_MMAP) != 0);
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}

TEST_CASE("Aes/simpleInsert", "")
{
  ham_env_t *env;
  ham_db_t *db;
  ham_parameter_t p[] = {
          { HAM_PARAM_ENCRYPTION_KEY, (uint64_t)"foo" },
          { 0, 0 }
  };

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0644, p));
  REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, 0));

  char buffer[512];
  ham_key_t key = {0};
  ham_record_t rec = {0};
  for (int i = 0; i < 512; i++) {
    key.data = &i;
    key.size = sizeof(i);
    rec.data = buffer;
    rec.size = i;
    for (int j = 0; j < i; j++)
      buffer[j] = (char)j;
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
  }

  for (int i = 0; i < 512; i++) {
    key.data = &i;
    key.size = sizeof(i);
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec, 0));
    REQUIRE((uint16_t)i == rec.size);
    REQUIRE((uint16_t)sizeof(i) == key.size);
    REQUIRE(i == *(int *)key.data);
    for (int j = 0; j < i; j++)
      REQUIRE((char)j == ((char *)rec.data)[j]);
  }

  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

  // reopen and check again
  REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"), 0, p));
  REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));

  for (int i = 0; i < 512; i++) {
    key.data = &i;
    key.size = sizeof(i);
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec, 0));
    REQUIRE((uint16_t)i == rec.size);
    REQUIRE((uint16_t)sizeof(i) == key.size);
    REQUIRE(i == *(int *)key.data);
    for (int j = 0; j < i; j++)
      REQUIRE((char)j == ((char *)rec.data)[j]);
  }

  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}

TEST_CASE("Aes/transactionInsert", "")
{
  ham_env_t *env;
  ham_db_t *db;
  ham_parameter_t p[] = {
          { HAM_PARAM_ENCRYPTION_KEY, (uint64_t)"foo" },
          { 0, 0 }
  };

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"),
                          HAM_ENABLE_TRANSACTIONS, 0644, p));
  REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, 0));

  char buffer[512];
  ham_key_t key = {0};
  ham_record_t rec = {0};
  for (int i = 0; i < 512; i++) {
    key.data = &i;
    key.size = sizeof(i);
    rec.data = buffer;
    rec.size = i;
    for (int j = 0; j < i; j++)
      buffer[j] = (char)j;
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
  }

  for (int i = 0; i < 512; i++) {
    key.data = &i;
    key.size = sizeof(i);
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec, 0));
    REQUIRE((uint16_t)i == rec.size);
    REQUIRE((uint16_t)sizeof(i) == key.size);
    REQUIRE(i == *(int *)key.data);
    for (int j = 0; j < i; j++)
      REQUIRE((char)j == ((char *)rec.data)[j]);
  }

  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

  // reopen and check again
  REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"),
                          HAM_ENABLE_TRANSACTIONS | HAM_AUTO_RECOVERY, p));
  REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));

  for (int i = 0; i < 512; i++) {
    key.data = &i;
    key.size = sizeof(i);
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec, 0));
    REQUIRE((uint16_t)i == rec.size);
    REQUIRE((uint16_t)sizeof(i) == key.size);
    REQUIRE(i == *(int *)key.data);
    for (int j = 0; j < i; j++)
      REQUIRE((char)j == ((char *)rec.data)[j]);
  }

  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}

