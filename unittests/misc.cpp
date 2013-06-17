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

#include <cstring>

#include "3rdparty/catch/catch.hpp"

#include "globals.h"

#include "../src/db.h"
#include "../src/page.h"
#include "../src/util.h"
#include "../src/btree.h"
#include "../src/env.h"
#include "../src/btree_key.h"

namespace hamsterdb {

struct MiscFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  BtreeIndex *m_btree;

  MiscFixture() {
    ham_parameter_t p[] = { { HAM_PARAM_PAGESIZE, 4096 }, { 0, 0 } };

    REQUIRE(0 ==
          ham_env_create(&m_env, 0, HAM_IN_MEMORY, 0644, &p[0]));
    REQUIRE(0 ==
          ham_env_create_db(m_env, &m_db, 1, 0, 0));

    Database *db = (Database *)m_db;
    m_btree = (BtreeIndex *)db->get_btree();
  }

  ~MiscFixture() {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void copyKeyTest() {
    ham_key_t src;
    ham_key_t dest = {};

    src.data = (void *)"hallo welt";
    src.size = (ham_u16_t)::strlen((char *)src.data) + 1;
    src.flags = 0;
    src._flags = 0;

    REQUIRE(0 == ((Database *)m_db)->copy_key(&src, &dest));
    REQUIRE(dest.size == src.size);
    REQUIRE(0 == ::strcmp((char *)dest.data, (char *)src.data));

    Memory::release(dest.data);
  }

  void copyExtendedKeyTest() {
    ham_key_t src;
    ham_key_t dest = {};

    src.data = (void *)"hallo welt, this is an extended key";
    src.size = (ham_u16_t)::strlen((char *)src.data) + 1;
    src.flags = 0;
    src._flags = 0;

    REQUIRE(0 == ((Database *)m_db)->copy_key(&src, &dest));
    REQUIRE(dest.size == src.size);
    REQUIRE(0 == ::strcmp((char *)dest.data, (char *)src.data));

    Memory::release(dest.data);
  }

  void copyKeyInt2PubEmptyTest() {
    PBtreeKey src;
    ham_key_t dest;
    memset(&src, 0, sizeof(src));
    memset(&dest, 0, sizeof(dest));

    src.set_ptr(0x12345);
    src.set_size(0);
    src.set_flags(0);

    REQUIRE(0 == m_btree->copy_key(&src, &dest));
    REQUIRE(0 == dest.size);
    REQUIRE((void *)0 == dest.data);
  }

  void copyKeyInt2PubTinyTest() {
    PBtreeKey src;
    ham_key_t dest;
    memset(&src, 0, sizeof(src));
    memset(&dest, 0, sizeof(dest));

    src.set_ptr(0x12345);
    src.set_size(1);
    src.set_flags(0);
    src._key[0] = 'a';

    REQUIRE(0 == m_btree->copy_key(&src, &dest));
    REQUIRE(1 == dest.size);
    REQUIRE('a' == ((char *)dest.data)[0]);
    Memory::release(dest.data);
  }

  void copyKeyInt2PubSmallTest() {
    char buffer[128];
    PBtreeKey *src = (PBtreeKey *)buffer;
    ham_key_t dest;
    memset(&dest, 0, sizeof(dest));

    src->set_ptr(0x12345);
    src->set_size(8);
    src->set_flags(0);
    ::memcpy((char *)src->_key, "1234567\0", 8);

    REQUIRE(0 == m_btree->copy_key(src, &dest));
    REQUIRE(dest.size == src->get_size());
    REQUIRE(0 == ::strcmp((char *)dest.data, (char *)src->_key));
    Memory::release(dest.data);
  }

  void copyKeyInt2PubFullTest() {
    char buffer[128];
    PBtreeKey *src = (PBtreeKey *)buffer;
    ham_key_t dest;
    memset(&dest, 0, sizeof(dest));

    src->set_ptr(0x12345);
    src->set_size(16);
    src->set_flags(0);
    ::strcpy((char *)&buffer[11] /*src->_key*/, "123456781234567\0");

    REQUIRE(0 == m_btree->copy_key(src, &dest));
    REQUIRE(dest.size == src->get_size());
    REQUIRE(0 == ::strcmp((char *)dest.data, (char *)src->_key));

    Memory::release(dest.data);
  }
};

TEST_CASE("MiscFixture/copyKeyTest",
           "Tests miscellaneous functions")
{
  MiscFixture mt;
  mt.copyKeyTest();
}

TEST_CASE("MiscFixture/copyExtendedKeyTest",
           "Tests miscellaneous functions")
{
  MiscFixture mt;
  mt.copyExtendedKeyTest();
}

TEST_CASE("MiscFixture/copyKeyInt2PubEmptyTest",
           "Tests miscellaneous functions")
{
  MiscFixture mt;
  mt.copyKeyInt2PubEmptyTest();
}

TEST_CASE("MiscFixture/copyKeyInt2PubTinyTest",
           "Tests miscellaneous functions")
{
  MiscFixture mt;
  mt.copyKeyInt2PubTinyTest();
}

TEST_CASE("MiscFixture/copyKeyInt2PubSmallTest",
           "Tests miscellaneous functions")
{
  MiscFixture mt;
  mt.copyKeyInt2PubSmallTest();
}

TEST_CASE("MiscFixture/copyKeyInt2PubFullTest",
           "Tests miscellaneous functions")
{
  MiscFixture mt;
  mt.copyKeyInt2PubFullTest();
}

} // namespace hamsterdb
