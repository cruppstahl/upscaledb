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

