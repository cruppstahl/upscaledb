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

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "1base/dynamic_array.h"
#include "2compressor/compressor_factory.h"

using namespace upscaledb;

TEST_CASE("Compression/factoryTest", "")
{
  Compressor *c;

#ifdef HAVE_ZLIB_H
  c = CompressorFactory::create(UPS_COMPRESSOR_ZLIB);
  REQUIRE(c != 0);
  delete c;
#endif

#ifdef HAVE_SNAPPY_H
  c = CompressorFactory::create(UPS_COMPRESSOR_SNAPPY);
  REQUIRE(c != 0);
  delete c;
#endif

  c = CompressorFactory::create(UPS_COMPRESSOR_LZF);
  REQUIRE(c != 0);
  delete c;
}

static void
simple_compressor_test(int library)
{
  Compressor *c = CompressorFactory::create(library);
  REQUIRE(c != 0);
  uint32_t len = c->compress((uint8_t *)"hello", 6);
  const uint8_t *ptr = c->arena.data();
  ByteArray tmp; // create a copy of ptr
  tmp.append(ptr, len);
  c->decompress(tmp.data(), len, 6);
  REQUIRE(!strcmp("hello", (const char *)ptr));
  delete c;
}

TEST_CASE("Compression/zlibTest", "")
{
#ifdef HAVE_ZLIB_H
  simple_compressor_test(UPS_COMPRESSOR_ZLIB);
#endif
}

TEST_CASE("Compression/snappyTest", "")
{
#ifdef HAVE_SNAPPY_H
  simple_compressor_test(UPS_COMPRESSOR_SNAPPY);
#endif
}

TEST_CASE("Compression/lzfTest", "")
{
  simple_compressor_test(UPS_COMPRESSOR_LZF);
}

static void
complex_journal_test(int library)
{
  ups_parameter_t params[] = {
    {UPS_PARAM_JOURNAL_COMPRESSION, (uint64_t)library},
    {0, 0}
  };
  ups_db_t *db;
  ups_env_t *env;
  REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"),
                          UPS_DONT_FLUSH_TRANSACTIONS | UPS_ENABLE_TRANSACTIONS,
                          0, &params[0]));
  REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, 0));

  char key_buffer[64] = {0};
  for (size_t i = 0; i < sizeof(key_buffer); i++)
    key_buffer[i] = (char)i;
  ups_key_t key = {0};
  key.data = &key_buffer[0];
  key.size = sizeof(key_buffer);

  char rec_buffer[1024] = {0};
  for (size_t i = 0; i < sizeof(rec_buffer); i++)
    rec_buffer[i] = (char)i + 10;
  ups_record_t rec = {0};
  rec.data = &rec_buffer[0];
  rec.size = sizeof(rec_buffer);

  for (int i = 0; i < 20; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
  }
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP | UPS_DONT_CLEAR_LOG));

  REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"),
                UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY, 0));
  REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));
  for (int i = 0; i < 20; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(!memcmp(rec_buffer, rec.data, rec.size));
  }

  params[0].value = 0;
  REQUIRE(0 == ups_env_get_parameters(env, &params[0]));
  REQUIRE(library == (int)params[0].value);

  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}

TEST_CASE("Compression/ZlibJournalTest", "")
{
#ifdef HAVE_ZLIB_H
  complex_journal_test(UPS_COMPRESSOR_ZLIB);
#endif
}

TEST_CASE("Compression/SnappyJournalTest", "")
{
#ifdef HAVE_SNAPPY_H
  complex_journal_test(UPS_COMPRESSOR_SNAPPY);
#endif
}

TEST_CASE("Compression/LzfJournalTest", "")
{
  complex_journal_test(UPS_COMPRESSOR_LZF);
}

static void
simple_record_test(int library)
{
  ups_parameter_t params[] = {
    {UPS_PARAM_RECORD_COMPRESSION, (uint64_t)library},
    {0, 0}
  };
  ups_db_t *m_db;
  ups_env_t *m_env;
  REQUIRE(0 == ups_env_create(&m_env, Utils::opath("test.db"), 0, 0, 0));
  REQUIRE(0 == ups_env_create_db(m_env, &m_db, 1, 0, &params[0]));

  char key_buffer[64] = {0};
  for (size_t i = 0; i < sizeof(key_buffer); i++)
    key_buffer[i] = (char)i;
  ups_key_t key = {0};
  key.data = &key_buffer[0];
  key.size = sizeof(key_buffer);

  char rec_buffer[1024] = {0};
  for (size_t i = 0; i < sizeof(rec_buffer); i++)
    rec_buffer[i] = (char)i + 10;
  ups_record_t rec = {0};
  rec.data = &rec_buffer[0];
  rec.size = sizeof(rec_buffer);

  // insert
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));
  }

  // lookup
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(0 == memcmp(rec.data, rec_buffer, sizeof(rec_buffer)));
  }

  // overwrite
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i + 10);
    // re-initialize record structure because ups_db_find overwrote it
    rec.data = &rec_buffer[0];
    rec.data = &rec_buffer[0];
    rec.size = sizeof(rec_buffer);
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, UPS_OVERWRITE));
  }

  // lookup
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i + 10);
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(0 == memcmp(rec.data, rec_buffer, sizeof(rec_buffer)));
  }

  REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
}

TEST_CASE("Compression/ZlibRecordTest", "")
{
#ifdef HAVE_ZLIB_H
  simple_record_test(UPS_COMPRESSOR_ZLIB);
#endif
}

TEST_CASE("Compression/SnappyRecordTest", "")
{
#ifdef HAVE_SNAPPY_H
  simple_record_test(UPS_COMPRESSOR_SNAPPY);
#endif
}

TEST_CASE("Compression/LzfRecordTest", "")
{
  simple_record_test(UPS_COMPRESSOR_LZF);
}

TEST_CASE("Compression/negativeOpenTest", "")
{
  ups_parameter_t params[] = {
    {UPS_PARAM_JOURNAL_COMPRESSION, UPS_COMPRESSOR_LZF},
    {0, 0}
  };
  ups_env_t *env;
  REQUIRE(UPS_INV_PARAMETER == ups_env_open(&env, Utils::opath("test.db"),
                UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY, &params[0]));
}

TEST_CASE("Compression/negativeOpenDbTest", "")
{
  ups_parameter_t params[] = {
    {UPS_PARAM_RECORD_COMPRESSION, UPS_COMPRESSOR_LZF},
    {0, 0}
  };
  ups_env_t *env;
  ups_db_t *db;
  REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"),
                UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY, 0));
  REQUIRE(UPS_INV_PARAMETER == ups_env_open_db(env, &db, 1, 0, &params[0]));
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}

static void
simple_key_test(int library)
{
  ups_parameter_t params[] = {
    {UPS_PARAM_RECORD_COMPRESSION, (uint64_t)library},
    {UPS_PARAM_KEY_COMPRESSION, (uint64_t)library},
    {0, 0}
  };
  ups_db_t *db;
  ups_env_t *env;
  REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
  REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, &params[0]));

  char key_buffer[64] = {0};
  for (size_t i = 0; i < sizeof(key_buffer); i++)
    key_buffer[i] = (char)i;
  ups_key_t key = {0};
  key.data = &key_buffer[0];
  key.size = sizeof(key_buffer);

  char rec_buffer[1024] = {0};
  for (size_t i = 0; i < sizeof(rec_buffer); i++)
    rec_buffer[i] = (char)i + 10;
  ups_record_t rec = {0};
  rec.data = &rec_buffer[0];
  rec.size = sizeof(rec_buffer);

  // insert
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
  }

  // lookup
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(0 == memcmp(rec.data, rec_buffer, sizeof(rec_buffer)));
  }

  // overwrite
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i + 10);
    // re-initialize record structure because ups_db_find overwrote it
    rec.data = &rec_buffer[0];
    rec.data = &rec_buffer[0];
    rec.size = sizeof(rec_buffer);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
  }

  // lookup
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i + 10);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(0 == memcmp(rec.data, rec_buffer, sizeof(rec_buffer)));
  }

  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

  REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, 0));
  REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));

  params[1].value = 0;
  REQUIRE(0 == ups_db_get_parameters(db, &params[1]));
  REQUIRE(library == (int)params[1].value);

  // lookup
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i + 10);
    REQUIRE(0 == ups_db_find(db, 0, &key, &rec, 0));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(0 == memcmp(rec.data, rec_buffer, sizeof(rec_buffer)));
  }

  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}

TEST_CASE("Compression/ZlibKeyTest", "")
{
#ifdef HAVE_ZLIB_H
  simple_key_test(UPS_COMPRESSOR_ZLIB);
#endif
}

TEST_CASE("Compression/SnappyKeyTest", "")
{
#ifdef HAVE_SNAPPY_H
  simple_key_test(UPS_COMPRESSOR_SNAPPY);
#endif
}

TEST_CASE("Compression/LzfKeyTest", "")
{
  simple_key_test(UPS_COMPRESSOR_LZF);
}

TEST_CASE("Compression/negativeKeyTest", "")
{
  ups_parameter_t param1[] = {
    {UPS_PARAM_KEY_COMPRESSION, UPS_COMPRESSOR_LZF},
    {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32},
    {0, 0}
  };

  ups_parameter_t param2[] = {
    {UPS_PARAM_KEY_COMPRESSION, UPS_COMPRESSOR_LZF},
    {UPS_PARAM_KEY_SIZE, 16},
    {0, 0}
  };

  ups_db_t *db;
  ups_env_t *env;

  REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
  REQUIRE(UPS_INV_PARAMETER == ups_env_create_db(env, &db, 1, 0, &param1[0]));
  REQUIRE(UPS_INV_PARAMETER == ups_env_create_db(env, &db, 1, 0, &param2[0]));
  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}

TEST_CASE("Compression/userAllocTest", "")
{
  ups_parameter_t params[] = {
    {UPS_PARAM_RECORD_COMPRESSION, UPS_COMPRESSOR_LZF},
    {UPS_PARAM_KEY_COMPRESSION, UPS_COMPRESSOR_LZF},
    {0, 0}
  };
  ups_db_t *db;
  ups_env_t *env;
  REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"),
                          UPS_IN_MEMORY, 0, 0));
  REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, &params[0]));

  char key_buffer[64] = {0};
  for (size_t i = 0; i < sizeof(key_buffer); i++)
    key_buffer[i] = (char)i;
  ups_key_t key = {0};
  key.data = &key_buffer[0];
  key.size = sizeof(key_buffer);

  char rec_buffer[1024] = {0};
  for (size_t i = 0; i < sizeof(rec_buffer); i++)
    rec_buffer[i] = (char)i + 10;
  ups_record_t rec = {0};
  rec.data = &rec_buffer[0];
  rec.size = sizeof(rec_buffer);

  // insert
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
  }

  ups_cursor_t *cursor;
  REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
  // lookup
  for (int i = 0; i < 5; i++) {
    key.flags = UPS_KEY_USER_ALLOC;
    rec.flags = UPS_RECORD_USER_ALLOC;
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(0 == memcmp(rec.data, rec_buffer, sizeof(rec_buffer)));
  }
  REQUIRE(0 == ups_cursor_close(cursor));

  // overwrite
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i + 10);
    // re-initialize record structure because ups_db_find overwrote it
    rec.data = &rec_buffer[0];
    rec.data = &rec_buffer[0];
    rec.size = sizeof(rec_buffer);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, UPS_OVERWRITE));
  }

  // lookup
  REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i + 10);
    REQUIRE(0 == ups_cursor_move(cursor, &key, &rec,
                            UPS_CURSOR_NEXT | UPS_DIRECT_ACCESS));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(0 == memcmp(rec.data, rec_buffer, sizeof(rec_buffer)));
  }
  REQUIRE(0 == ups_cursor_close(cursor));

  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}

TEST_CASE("Compression/unknownCompressorTest", "")
{
  ups_parameter_t params[] = {
    {UPS_PARAM_RECORD_COMPRESSION, 44},
    {UPS_PARAM_KEY_COMPRESSION, 55},
    {0, 0}
  };
  ups_db_t *db;
  ups_env_t *env;
  REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"),
                          UPS_IN_MEMORY, 0, 0));
  REQUIRE(UPS_INV_PARAMETER == ups_env_create_db(env, &db, 1, 0, &params[0]));

  REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
}
