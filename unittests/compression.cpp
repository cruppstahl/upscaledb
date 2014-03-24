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

#ifdef HAM_ENABLE_COMPRESSION

#include "../src/config.h"

#include "3rdparty/catch/catch.hpp"

#include "globals.h"

#include "../src/util.h"
#include "../src/compressor_factory.h"

using namespace hamsterdb;

TEST_CASE("Compression/factoryTest", "")
{
  Compressor *c;

#ifdef HAVE_ZLIB_H
  c = CompressorFactory::create(HAM_COMPRESSOR_ZLIB);
  REQUIRE(c != 0);
  delete c;
#endif

#ifdef HAVE_SNAPPY_H
  c = CompressorFactory::create(HAM_COMPRESSOR_SNAPPY);
  REQUIRE(c != 0);
  delete c;
#endif

#ifdef HAVE_LZO_LZO1X_H
  c = CompressorFactory::create(HAM_COMPRESSOR_LZO);
  REQUIRE(c != 0);
  delete c;
#endif

  c = CompressorFactory::create(HAM_COMPRESSOR_LZF);
  REQUIRE(c != 0);
  delete c;
}

static void
simple_compressor_test(int library)
{
  Compressor *c = CompressorFactory::create(library);
  REQUIRE(c != 0);
  ham_u32_t len = c->compress((ham_u8_t *)"hello", 6);
  const ham_u8_t *ptr = c->get_output_data();
  ByteArray tmp; // create a copy of ptr
  tmp.append(ptr, len);
  c->decompress((ham_u8_t *)tmp.get_ptr(), len, 6);
  REQUIRE(!strcmp("hello", (const char *)ptr));
  delete c;
}

TEST_CASE("Compression/zlibTest", "")
{
#ifdef HAVE_ZLIB_H
  simple_compressor_test(HAM_COMPRESSOR_ZLIB);
#endif
}

TEST_CASE("Compression/snappyTest", "")
{
#ifdef HAVE_SNAPPY_H
  simple_compressor_test(HAM_COMPRESSOR_SNAPPY);
#endif
}

TEST_CASE("Compression/lzfTest", "")
{
  simple_compressor_test(HAM_COMPRESSOR_LZF);
}

TEST_CASE("Compression/lzopTest", "")
{
#ifdef HAVE_LZO_LZO1X_H
  simple_compressor_test(HAM_COMPRESSOR_LZO);
#endif
}

static void
complex_journal_test(int library)
{
  ham_parameter_t params[] = {
    {HAM_PARAM_JOURNAL_COMPRESSION, (ham_u64_t)library},
    {0, 0}
  };
  ham_db_t *m_db;
  ham_env_t *m_env;
  REQUIRE(0 == ham_env_create(&m_env, Globals::opath("test.db"),
                          HAM_ENABLE_TRANSACTIONS, 0, &params[0]));
  REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, 0));

  char key_buffer[64] = {0};
  for (size_t i = 0; i < sizeof(key_buffer); i++)
    key_buffer[i] = (char)i;
  ham_key_t key = {0};
  key.data = &key_buffer[0];
  key.size = sizeof(key_buffer);

  char rec_buffer[1024] = {0};
  for (size_t i = 0; i < sizeof(rec_buffer); i++)
    rec_buffer[i] = (char)i + 10;
  ham_record_t rec = {0};
  rec.data = &rec_buffer[0];
  rec.size = sizeof(rec_buffer);

  for (int i = 0; i < 20; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
  }
  REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP | HAM_DONT_CLEAR_LOG));

  REQUIRE(0 == ham_env_open(&m_env, Globals::opath("test.db"),
                HAM_ENABLE_TRANSACTIONS | HAM_AUTO_RECOVERY, &params[0]));
  REQUIRE(0 == ham_env_open_db(m_env, &m_db, 1, 0, 0));
  for (int i = 0; i < 20; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(!memcmp(rec_buffer, rec.data, rec.size));
  }
  REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
}

TEST_CASE("Compression/ZlibJournalTest", "")
{
#ifdef HAVE_ZLIB_H
  complex_journal_test(HAM_COMPRESSOR_ZLIB);
#endif
}

TEST_CASE("Compression/SnappyJournalTest", "")
{
#ifdef HAVE_SNAPPY_H
  complex_journal_test(HAM_COMPRESSOR_SNAPPY);
#endif
}

TEST_CASE("Compression/LzoJournalTest", "")
{
#ifdef HAVE_LZO_LZO1X_H
  complex_journal_test(HAM_COMPRESSOR_LZO);
#endif
}

TEST_CASE("Compression/LzfJournalTest", "")
{
  complex_journal_test(HAM_COMPRESSOR_LZF);
}

TEST_CASE("Compression/partialTest", "")
{
  ham_parameter_t params[] = {
          {HAM_PARAM_RECORD_COMPRESSION, HAM_COMPRESSOR_ZLIB},
          {0, 0}
  };
  ham_db_t *db;
  ham_env_t *env;
  REQUIRE(0 == ham_env_create(&env, Globals::opath("test.db"), 0, 0, 0));
  REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, &params[0]));

  ham_key_t key = {0};
  ham_record_t rec = {0};
  key.data = (void *)"hello world";
  key.size = 12;

  char buffer[100] = {0};

  /* write the record */
  rec.data = buffer;
  rec.size = sizeof(buffer);
  rec.partial_size = 50;
  REQUIRE(HAM_INV_PARAMETER == ham_db_insert(db, 0, &key, &rec, HAM_PARTIAL));

  REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
  REQUIRE(HAM_INV_PARAMETER == ham_db_find(db, 0, &key, &rec, HAM_PARTIAL));

  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}

static void
simple_record_test(int library)
{
  ham_parameter_t params[] = {
    {HAM_PARAM_RECORD_COMPRESSION, (ham_u64_t)library},
    {0, 0}
  };
  ham_db_t *m_db;
  ham_env_t *m_env;
  REQUIRE(0 == ham_env_create(&m_env, Globals::opath("test.db"), 0, 0, 0));
  REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, &params[0]));

  char key_buffer[64] = {0};
  for (size_t i = 0; i < sizeof(key_buffer); i++)
    key_buffer[i] = (char)i;
  ham_key_t key = {0};
  key.data = &key_buffer[0];
  key.size = sizeof(key_buffer);

  char rec_buffer[1024] = {0};
  for (size_t i = 0; i < sizeof(rec_buffer); i++)
    rec_buffer[i] = (char)i + 10;
  ham_record_t rec = {0};
  rec.data = &rec_buffer[0];
  rec.size = sizeof(rec_buffer);

  // insert
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
  }

  // lookup
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(0 == memcmp(rec.data, rec_buffer, sizeof(rec_buffer)));
  }

  // overwrite
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i + 10);
    // re-initialize record structure because ham_db_find overwrote it
    rec.data = &rec_buffer[0];
    rec.data = &rec_buffer[0];
    rec.size = sizeof(rec_buffer);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
  }

  // lookup
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i + 10);
    REQUIRE(0 == ham_db_find(m_db, 0, &key, &rec, 0));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(0 == memcmp(rec.data, rec_buffer, sizeof(rec_buffer)));
  }

  REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
}

TEST_CASE("Compression/ZlibRecordTest", "")
{
#ifdef HAVE_ZLIB_H
  simple_record_test(HAM_COMPRESSOR_ZLIB);
#endif
}

TEST_CASE("Compression/SnappyRecordTest", "")
{
#ifdef HAVE_SNAPPY_H
  simple_record_test(HAM_COMPRESSOR_SNAPPY);
#endif
}

TEST_CASE("Compression/LzoRecordTest", "")
{
#ifdef HAVE_LZO_LZO1X_H
  simple_record_test(HAM_COMPRESSOR_LZO);
#endif
}

TEST_CASE("Compression/LzfRecordTest", "")
{
  simple_record_test(HAM_COMPRESSOR_LZF);
}

static void
simple_key_test(int library)
{
  ham_parameter_t params[] = {
    {HAM_PARAM_RECORD_COMPRESSION, HAM_COMPRESSOR_SNAPPY},
    {HAM_PARAM_KEY_COMPRESSION, (ham_u64_t)library},
    {0, 0}
  };
  ham_db_t *db;
  ham_env_t *env;
  REQUIRE(0 == ham_env_create(&env, Globals::opath("test.db"), 0, 0, 0));
  REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, &params[0]));

  char key_buffer[64] = {0};
  for (size_t i = 0; i < sizeof(key_buffer); i++)
    key_buffer[i] = (char)i;
  ham_key_t key = {0};
  key.data = &key_buffer[0];
  key.size = sizeof(key_buffer);

  char rec_buffer[1024] = {0};
  for (size_t i = 0; i < sizeof(rec_buffer); i++)
    rec_buffer[i] = (char)i + 10;
  ham_record_t rec = {0};
  rec.data = &rec_buffer[0];
  rec.size = sizeof(rec_buffer);

  // insert
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
  }

  // lookup
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i);
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec, 0));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(0 == memcmp(rec.data, rec_buffer, sizeof(rec_buffer)));
  }

  // overwrite
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i + 10);
    // re-initialize record structure because ham_db_find overwrote it
    rec.data = &rec_buffer[0];
    rec.data = &rec_buffer[0];
    rec.size = sizeof(rec_buffer);
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, HAM_OVERWRITE));
  }

  // lookup
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i + 10);
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec, 0));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(0 == memcmp(rec.data, rec_buffer, sizeof(rec_buffer)));
  }

  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

  REQUIRE(0 == ham_env_open(&env, Globals::opath("test.db"), 0, 0));
  REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));

  params[1].value = 0;
  REQUIRE(0 == ham_db_get_parameters(db, &params[1]));
  REQUIRE(library == (int)params[1].value);

  // lookup
  for (int i = 0; i < 5; i++) {
    sprintf(key_buffer, "%02d", i);
    sprintf(rec_buffer, "%02d", i + 10);
    REQUIRE(0 == ham_db_find(db, 0, &key, &rec, 0));
    REQUIRE(rec.size == sizeof(rec_buffer));
    REQUIRE(0 == memcmp(rec.data, rec_buffer, sizeof(rec_buffer)));
  }

  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}

TEST_CASE("Compression/ZlibKeyTest", "")
{
#ifdef HAVE_ZLIB_H
  simple_key_test(HAM_COMPRESSOR_ZLIB);
#endif
}

TEST_CASE("Compression/SnappyKeyTest", "")
{
#ifdef HAVE_SNAPPY_H
  simple_key_test(HAM_COMPRESSOR_SNAPPY);
#endif
}

TEST_CASE("Compression/LzoKeyTest", "")
{
#ifdef HAVE_LZO_LZO1X_H
  simple_key_test(HAM_COMPRESSOR_LZO);
#endif
}

TEST_CASE("Compression/LzfKeyTest", "")
{
  simple_key_test(HAM_COMPRESSOR_LZF);
}

TEST_CASE("Compression/negativeKeyTest", "")
{
  ham_parameter_t param1[] = {
      {HAM_PARAM_KEY_COMPRESSION, HAM_COMPRESSOR_LZF},
      {HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT32},
      {0, 0}
  };

  ham_parameter_t param2[] = {
      {HAM_PARAM_KEY_COMPRESSION, HAM_COMPRESSOR_LZF},
      {HAM_PARAM_KEY_SIZE, 16},
      {0, 0}
  };

  ham_db_t *db;
  ham_env_t *env;

  REQUIRE(0 == ham_env_create(&env, Globals::opath("test.db"), 0, 0, 0));
  REQUIRE(HAM_INV_PARAMETER == ham_env_create_db(env, &db, 1, 0, &param1[0]));
  REQUIRE(HAM_INV_PARAMETER == ham_env_create_db(env, &db, 1, 0, &param2[0]));

  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}


#endif // HAM_ENABLE_COMPRESSION
