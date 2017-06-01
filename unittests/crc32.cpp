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

#include "1os/file.h"

using namespace upscaledb;

static inline void
garbagify_file(const char *filename, uint64_t address)
{
  File f;
  f.open(filename, 0);
  f.pwrite(address, "xxx", 3);
  f.close();
}

TEST_CASE("Crc32/disabledIfInMemory")
{
  BaseFixture f;
  f.require_create(UPS_ENABLE_CRC32 | UPS_IN_MEMORY, UPS_INV_PARAMETER)
   .close();
}

TEST_CASE("Crc32/notPersistentFlag")
{
  BaseFixture f;
  f.require_create(UPS_ENABLE_CRC32)
   .require_flags(UPS_ENABLE_CRC32)
   .close();

  f.require_open()
   .require_flags(UPS_ENABLE_CRC32, false)
   .close();

  f.require_open(UPS_ENABLE_CRC32)
   .require_flags(UPS_ENABLE_CRC32)
   .close();
}

TEST_CASE("Crc32/corruptPageTest", "")
{
  BaseFixture f;
  f.require_create(UPS_ENABLE_CRC32)
   .require_flags(UPS_ENABLE_CRC32);

  DbProxy db(f.db);
  db.require_insert("1", nullptr);
  f.close();

  // flip a few bytes in page 16 * 1024
  garbagify_file("test.db", 1024 * 16 + 200);

  f.require_open(UPS_ENABLE_CRC32);

  db = DbProxy(f.db);
  db.require_find("1", nullptr, UPS_INTEGRITY_VIOLATED);
}

TEST_CASE("Crc32/multipageBlobTest", "")
{
  std::vector<uint8_t> v1(1024 * 32, 0);
  std::vector<uint8_t> v2(1024 * 32, 1);

  BaseFixture f;
  f.require_create(UPS_ENABLE_CRC32)
   .require_flags(UPS_ENABLE_CRC32);

  // insert and verify
  DbProxy db(f.db);
  db.require_insert("1", v1)
    .require_find("1", v1);

  // reopen and verify
  f.close()
   .require_open(UPS_ENABLE_CRC32);

  db = DbProxy(f.db);
  db.require_find("1", v1);

  // overwrite and verify
  db.require_overwrite("1", v2)
    .require_find("1", v2);
  
  // reopen and verify once more
  f.close()
   .require_open(UPS_ENABLE_CRC32);

  db = DbProxy(f.db);
  db.require_find("1", v2);
}

TEST_CASE("Crc32/corruptMultipageBlobTest", "")
{
  std::vector<uint8_t> v1(1024 * 32, 0);

  BaseFixture f;
  f.require_create(UPS_ENABLE_CRC32)
   .require_flags(UPS_ENABLE_CRC32);

  // insert and verify
  DbProxy db(f.db);
  db.require_insert("1", v1)
    .require_find("1", v1);
  f.close();

  // flip a few bytes in page 32 * 1024
  garbagify_file("test.db", 1024 * 32 + 200);

  f.require_open(UPS_ENABLE_CRC32);
  db = DbProxy(f.db);
  db.require_find("1", v1, UPS_INTEGRITY_VIOLATED);
}

