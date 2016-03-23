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

