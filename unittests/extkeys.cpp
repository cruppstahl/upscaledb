/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "../src/config.h"

#include "3rdparty/catch/catch.hpp"

#include "globals.h"

#include "../src/db.h"
#include "../src/extkeys.h"
#include "../src/env.h"

using namespace hamsterdb;

struct ExtendedKeyFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;

  ExtendedKeyFixture() {
    REQUIRE(0 == ham_env_create(&m_env, 0, HAM_IN_MEMORY, 0, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, 0));

    ExtKeyCache *c = new ExtKeyCache((Database *)m_db);
    REQUIRE(c != 0);
    ((Database *)m_db)->set_extkey_cache(c);
  }

  ~ExtendedKeyFixture() {
    REQUIRE(0 == ham_env_close(m_env, 0));
  }
};

TEST_CASE("ExtKey/insertFetchRemove", "")
{
  ExtendedKeyFixture f;
  ExtKeyCache *c = ((Database *)f.m_db)->get_extkey_cache();
  ham_u8_t *pbuffer = 0, buffer[12] = {0};
  ham_size_t size = 0;

  c->insert(0x123, sizeof(buffer), buffer);

  REQUIRE(0 == c->fetch(0x123, &size, &pbuffer));
  REQUIRE((ham_size_t)12 == size);
  REQUIRE(0 == ::memcmp(pbuffer, buffer, size));

  c->remove(0x123);
}

TEST_CASE("ExtKey/negativeFetch", "")
{
  ExtendedKeyFixture f;
  ExtKeyCache *c = ((Database *)f.m_db)->get_extkey_cache();
  ham_u8_t *pbuffer = 0, buffer[12] = {0};
  ham_size_t size = 0;

  c->insert(0x123, sizeof(buffer), buffer);
  REQUIRE(HAM_KEY_NOT_FOUND == c->fetch(0x321, &size, &pbuffer));

  REQUIRE(0 == c->fetch(0x123, &size, &pbuffer));
  REQUIRE((ham_size_t)12 == size);
  REQUIRE(0 == ::memcmp(pbuffer, buffer, size));

  c->remove(0x123);
  REQUIRE(HAM_KEY_NOT_FOUND == c->fetch(0x123, &size, &pbuffer));
}

TEST_CASE("ExtKey/bigCache", "")
{
  ExtendedKeyFixture f;
  ExtKeyCache *c = ((Database *)f.m_db)->get_extkey_cache();
  ham_u8_t *pbuffer = 0, buffer[12] = {0};
  ham_size_t size = 0;

  for (ham_size_t i = 0; i < 10000; i++)
    c->insert((ham_u64_t)i, sizeof(buffer), buffer);

  for (ham_size_t i = 0; i < 10000; i++) {
    REQUIRE(0 == c->fetch((ham_u64_t)i, &size, &pbuffer));
    REQUIRE((ham_size_t)12 == size);
  }

  for (ham_size_t i = 0; i < 10000; i++)
     c->remove((ham_u64_t)i);

  for (ham_size_t i = 0; i < 10000; i++)
    REQUIRE(HAM_KEY_NOT_FOUND == c->fetch((ham_u64_t)i, 0, 0));
}

TEST_CASE("ExtKey/purge", "")
{
  ExtendedKeyFixture f;
  ExtKeyCache *c = ((Database *)f.m_db)->get_extkey_cache();
  ham_u8_t *pbuffer, buffer[12] = {0};
  ham_size_t size;

  for (int i = 0; i < 20; i++)
    c->insert((ham_u64_t)i, sizeof(buffer), buffer);

  ham_env_t *env = ham_db_get_env(f.m_db);
  Environment *e = (Environment *)env;
  e->set_txn_id(e->get_txn_id() + 2000);

  c->purge();

  for (int i = 0; i < 20; i++)
    REQUIRE(HAM_KEY_NOT_FOUND == c->fetch((ham_u64_t)i, &size, &pbuffer));
}

