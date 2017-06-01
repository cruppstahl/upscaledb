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

#include "fixture.hpp"

#include "1base/dynamic_array.h"
#include "2compressor/compressor_factory.h"

using namespace upscaledb;

TEST_CASE("Compression/factory", "")
{
  ScopedPtr<Compressor> c;

#ifdef HAVE_ZLIB_H
  c.reset(CompressorFactory::create(UPS_COMPRESSOR_ZLIB));
  REQUIRE(c.get() != nullptr);
#endif

#ifdef HAVE_SNAPPY_H
  c.reset(CompressorFactory::create(UPS_COMPRESSOR_SNAPPY));
  REQUIRE(c.get() != nullptr);
#endif

  c.reset(CompressorFactory::create(UPS_COMPRESSOR_LZF));
  REQUIRE(c.get() != nullptr);
}

static void
simple_compressor_test(int library)
{
  ScopedPtr<Compressor> c(CompressorFactory::create(library));
  REQUIRE(c.get() != nullptr);
  uint32_t len = c->compress((uint8_t *)"hello", 6);
  const uint8_t *ptr = c->arena.data();

  ByteArray tmp; // create a copy of ptr
  tmp.append(ptr, len);
  c->decompress(tmp.data(), len, 6);
  REQUIRE(0 == ::strcmp("hello", (const char *)ptr));
}

TEST_CASE("Compression/zlib", "")
{
#ifdef HAVE_ZLIB_H
  simple_compressor_test(UPS_COMPRESSOR_ZLIB);
#endif
}

TEST_CASE("Compression/snappy", "")
{
#ifdef HAVE_SNAPPY_H
  simple_compressor_test(UPS_COMPRESSOR_SNAPPY);
#endif
}

TEST_CASE("Compression/lzf", "")
{
  simple_compressor_test(UPS_COMPRESSOR_LZF);
}

static void
complex_journal_test(int library)
{
  ups_parameter_t p[] = {
      { UPS_PARAM_JOURNAL_COMPRESSION, (uint64_t)library },
      { 0, 0 }
  };

  BaseFixture f;
  f.require_create(UPS_DONT_FLUSH_TRANSACTIONS | UPS_ENABLE_TRANSACTIONS, p);

  std::vector<uint8_t> kvec(64);
  for (size_t i = 0; i < kvec.size(); i++)
    kvec[i] = (uint8_t)i;

  std::vector<uint8_t> rvec(1024);
  for (size_t i = 0; i < rvec.size(); i++)
    rvec[i] = (uint8_t)i + 10;

  DbProxy db(f.db);
  for (int i = 0; i < 20; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i);
    db.require_insert(kvec, rvec);
  }

  // reopen, perform recovery
  f.close(UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG)
   .require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY);
  db = DbProxy(f.db);

  for (int i = 0; i < 20; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i);
    db.require_find(kvec, rvec);
  }
  f.require_parameter(UPS_PARAM_JOURNAL_COMPRESSION, library);
}

TEST_CASE("Compression/ZlibJournal", "")
{
#ifdef HAVE_ZLIB_H
  complex_journal_test(UPS_COMPRESSOR_ZLIB);
#endif
}

TEST_CASE("Compression/SnappyJournal", "")
{
#ifdef HAVE_SNAPPY_H
  complex_journal_test(UPS_COMPRESSOR_SNAPPY);
#endif
}

TEST_CASE("Compression/LzfJournal", "")
{
  complex_journal_test(UPS_COMPRESSOR_LZF);
}

static void
simple_record_test(int library)
{
  ups_parameter_t p[] = {
      { UPS_PARAM_JOURNAL_COMPRESSION, (uint64_t)library },
      { 0, 0 }
  };

  BaseFixture f;
  f.require_create(0, p);

  std::vector<uint8_t> kvec(64);
  for (size_t i = 0; i < kvec.size(); i++)
    kvec[i] = (uint8_t)i;

  std::vector<uint8_t> rvec(1024);
  for (size_t i = 0; i < rvec.size(); i++)
    rvec[i] = (uint8_t)i + 10;

  DbProxy db(f.db);
  for (int i = 0; i < 20; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i);
    db.require_insert(kvec, rvec);
  }
  for (int i = 0; i < 20; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i);
    db.require_find(kvec, rvec);
  }
  for (int i = 0; i < 20; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i + 10);
    db.require_overwrite(kvec, rvec);
  }
  for (int i = 0; i < 20; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i + 10);
    db.require_find(kvec, rvec);
  }
}

TEST_CASE("Compression/ZlibRecord", "")
{
#ifdef HAVE_ZLIB_H
  simple_record_test(UPS_COMPRESSOR_ZLIB);
#endif
}

TEST_CASE("Compression/SnappyRecord", "")
{
#ifdef HAVE_SNAPPY_H
  simple_record_test(UPS_COMPRESSOR_SNAPPY);
#endif
}

TEST_CASE("Compression/LzfRecord", "")
{
  simple_record_test(UPS_COMPRESSOR_LZF);
}

TEST_CASE("Compression/negativeOpen", "")
{
  ups_parameter_t p[] = {
      { UPS_PARAM_JOURNAL_COMPRESSION, UPS_COMPRESSOR_LZF },
      { 0, 0 }
  };

  BaseFixture f;
  f.require_open(UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY, p,
                  UPS_INV_PARAMETER);
}

TEST_CASE("Compression/negativeOpenDb", "")
{
  ups_parameter_t params[] = {
    {UPS_PARAM_RECORD_COMPRESSION, UPS_COMPRESSOR_LZF},
    {0, 0}
  };
  ups_env_t *env;
  ups_db_t *db;
  REQUIRE(0 == ups_env_open(&env, "test.db",
                UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY, 0));
  REQUIRE(UPS_INV_PARAMETER == ups_env_open_db(env, &db, 1, 0, &params[0]));
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}

static void
simple_key_test(int library)
{
  ups_parameter_t params[] = {
      { UPS_PARAM_RECORD_COMPRESSION, (uint64_t)library },
      { UPS_PARAM_KEY_COMPRESSION, (uint64_t)library },
      { 0, 0 }
  };

  BaseFixture f;
  f.require_create(0, 0, 0, params);

  DbProxy db(f.db);
  db.require_parameter(UPS_PARAM_KEY_COMPRESSION, library)
    .require_parameter(UPS_PARAM_RECORD_COMPRESSION, library);

  std::vector<uint8_t> kvec(64);
  for (size_t i = 0; i < kvec.size(); i++)
    kvec[i] = (uint8_t)i;

  std::vector<uint8_t> rvec(1024);
  for (size_t i = 0; i < rvec.size(); i++)
    rvec[i] = (uint8_t)i + 10;

  // insert
  for (int i = 0; i < 5; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i);
    db.require_insert(kvec, rvec);
  }

  // lookup
  for (int i = 0; i < 5; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i);
    db.require_find(kvec, rvec);
  }

  // overwrite
  for (int i = 0; i < 5; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i + 10);
    db.require_overwrite(kvec, rvec);
  }

  // lookup
  for (int i = 0; i < 5; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i + 10);
    db.require_find(kvec, rvec);
  }

  f.close()
   .require_open();
  db = DbProxy(f.db);
  db.require_parameter(UPS_PARAM_KEY_COMPRESSION, library)
    .require_parameter(UPS_PARAM_RECORD_COMPRESSION, library);
   
  // lookup
  for (int i = 0; i < 5; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i + 10);
    db.require_find(kvec, rvec);
  }
}

TEST_CASE("Compression/ZlibKey", "")
{
#ifdef HAVE_ZLIB_H
  simple_key_test(UPS_COMPRESSOR_ZLIB);
#endif
}

TEST_CASE("Compression/SnappyKey", "")
{
#ifdef HAVE_SNAPPY_H
  simple_key_test(UPS_COMPRESSOR_SNAPPY);
#endif
}

TEST_CASE("Compression/LzfKey", "")
{
  simple_key_test(UPS_COMPRESSOR_LZF);
}

TEST_CASE("Compression/negativeKey", "")
{
  ups_parameter_t param1[] = {
      { UPS_PARAM_KEY_COMPRESSION, UPS_COMPRESSOR_LZF },
      { UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32 },
      { 0, 0 }
  };

  ups_parameter_t param2[] = {
      { UPS_PARAM_KEY_COMPRESSION, UPS_COMPRESSOR_LZF },
      { UPS_PARAM_KEY_SIZE, 16 },
      { 0, 0 }
  };

  BaseFixture f;
  f.require_create(0, 0, 0, param1, UPS_INV_PARAMETER)
   .require_create(0, 0, 0, param2, UPS_INV_PARAMETER);
}

TEST_CASE("Compression/userAlloc", "")
{
  ups_parameter_t params[] = {
      { UPS_PARAM_RECORD_COMPRESSION, UPS_COMPRESSOR_LZF },
      { UPS_PARAM_KEY_COMPRESSION, UPS_COMPRESSOR_LZF },
      { 0, 0 }
  };

  BaseFixture f;
  f.require_create(0, 0, 0, params);

  std::vector<uint8_t> kvec(64);
  for (size_t i = 0; i < kvec.size(); i++)
    kvec[i] = (uint8_t)i;

  std::vector<uint8_t> rvec(1024);
  for (size_t i = 0; i < rvec.size(); i++)
    rvec[i] = (uint8_t)i + 10;

  DbProxy db(f.db);

  // insert
  for (int i = 0; i < 20; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i);
    db.require_insert(kvec, rvec);
  }

  // verify
  for (int i = 0; i < 20; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i);
    db.require_find_useralloc(kvec, rvec);
  }

  // overwrite
  for (int i = 0; i < 20; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i + 10);
    db.require_overwrite(kvec, rvec);
  }

  // verify
  for (int i = 0; i < 20; i++) {
    ::sprintf((char *)&kvec[0], "%02d", i);
    ::sprintf((char *)&rvec[0], "%02d", i + 10);
    db.require_find_useralloc(kvec, rvec);
  }
}

TEST_CASE("Compression/unknownCompressor", "")
{
  ups_parameter_t params[] = {
      { UPS_PARAM_RECORD_COMPRESSION, 44 },
      { UPS_PARAM_KEY_COMPRESSION, 55 },
      { 0, 0 }
  };

  BaseFixture f;
  f.require_create(UPS_IN_MEMORY, 0, 0, params, UPS_INV_PARAMETER);
}
