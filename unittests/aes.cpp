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

#include <string.h>
#include <assert.h>

#include "3rdparty/catch/catch.hpp"

#include "fixture.hpp"

#include "4env/env_local.h"

using namespace upscaledb;

TEST_CASE("Aes/disabledIfInMemory", "")
{
  ups_parameter_t p[] = {
          { UPS_PARAM_ENCRYPTION_KEY, (uint64_t)"foo" },
          { 0, 0 }
  };

  BaseFixture f;
  f.require_create(UPS_IN_MEMORY, p, UPS_INV_PARAMETER)
   .close();
}

TEST_CASE("Aes/disableMmap", "")
{
  ups_parameter_t p[] = {
          { UPS_PARAM_ENCRYPTION_KEY, (uint64_t)"foo" },
          { 0, 0 }
  };
  ups_parameter_t bad[] = {
          { UPS_PARAM_ENCRYPTION_KEY, (uint64_t)"bar" },
          { 0, 0 }
  };

  BaseFixture f;
  f.require_create(0, p)
   .require_flags(UPS_DISABLE_MMAP)
   .close();

  f.require_open(0, 0, UPS_INV_FILE_HEADER) 
   .require_open(0, bad, UPS_INV_FILE_HEADER)
   .require_open(0, p)
   .require_flags(UPS_DISABLE_MMAP);
}

TEST_CASE("Aes/simpleInsert", "")
{
  ups_parameter_t p[] = {
          { UPS_PARAM_ENCRYPTION_KEY, (uint64_t)"foo" },
          { 0, 0 }
  };

  BaseFixture f;
  f.require_create(0, p)
   .require_flags(UPS_DISABLE_MMAP);

  DbProxy db(f.db);
  for (uint32_t i = 0; i < 512; i++) {
    std::vector<uint8_t> buffer(512, (uint8_t)i);
    db.require_insert(i, buffer);
  }
  for (uint32_t i = 0; i < 512; i++) {
    std::vector<uint8_t> buffer(512, (uint8_t)i);
    db.require_find(i, buffer);
  }

  // reopen and check again
  f.close()
   .require_open(0, p);
  db = DbProxy(f.db);
  for (uint32_t i = 0; i < 512; i++) {
    std::vector<uint8_t> buffer(512, (uint8_t)i);
    db.require_find(i, buffer);
  }
}

TEST_CASE("Aes/transactionInsert", "")
{
  ups_parameter_t p[] = {
          { UPS_PARAM_ENCRYPTION_KEY, (uint64_t)"foo" },
          { 0, 0 }
  };

  BaseFixture f;
  f.require_create(UPS_ENABLE_TRANSACTIONS, p)
   .require_flags(UPS_DISABLE_MMAP);

  DbProxy db(f.db);
  for (uint32_t i = 0; i < 512; i++) {
    std::vector<uint8_t> buffer(512, (uint8_t)i);
    db.require_insert(i, buffer);
  }

  // reopen and check again
  f.close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG)
   .require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY, p);
  db = DbProxy(f.db);
  for (uint32_t i = 0; i < 512; i++) {
    std::vector<uint8_t> buffer(512, (uint8_t)i);
    db.require_find(i, buffer);
  }
}

