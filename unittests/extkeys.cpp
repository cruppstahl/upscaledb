/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "../src/config.h"

#include <stdexcept>
#include <cstring>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/extkeys.h"
#include "../src/env.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace ham;

class ExtendedKeyTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    ExtendedKeyTest()
    :   hamsterDB_fixture("ExtendedKeyTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(ExtendedKeyTest, insertFetchRemoveTest);
        BFC_REGISTER_TEST(ExtendedKeyTest, negativeFetchTest);
        BFC_REGISTER_TEST(ExtendedKeyTest, bigCacheTest);
        BFC_REGISTER_TEST(ExtendedKeyTest, purgeTest);
    }

protected:
    ham_db_t *m_db;

public:
    virtual void setup()
    {
        __super::setup();

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, ham_create(m_db, 0, HAM_IN_MEMORY, 0));

        ExtKeyCache *c=new ExtKeyCache((Database *)m_db);
        BFC_ASSERT(c!=0);
        ((Database *)m_db)->set_extkey_cache(c);
    }

    virtual void teardown()
    {
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
    }

    void insertFetchRemoveTest(void)
    {
        ExtKeyCache *c=((Database *)m_db)->get_extkey_cache();
        ham_u8_t *pbuffer, buffer[12]={0};
        ham_size_t size;

        c->insert(0x123, sizeof(buffer), buffer);

        BFC_ASSERT_EQUAL(0, c->fetch(0x123, &size, &pbuffer));
        BFC_ASSERT_EQUAL((ham_size_t)12, size);
        BFC_ASSERT(::memcmp(pbuffer, buffer, size)==0);

        c->remove(0x123);
    }

    void negativeFetchTest(void)
    {
        ExtKeyCache *c=((Database *)m_db)->get_extkey_cache();
        ham_u8_t *pbuffer=0, buffer[12]={0};
        ham_size_t size=0;

        c->insert(0x123, sizeof(buffer), buffer);
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, c->fetch(0x321, &size, &pbuffer));

        BFC_ASSERT_EQUAL(0, c->fetch(0x123, &size, &pbuffer));
        BFC_ASSERT_EQUAL((ham_size_t)12, size);
        BFC_ASSERT(::memcmp(pbuffer, buffer, size)==0);

        c->remove(0x123);
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, c->fetch(0x123, &size, &pbuffer));
    }

    void bigCacheTest(void)
    {
        ExtKeyCache *c=((Database *)m_db)->get_extkey_cache();
        ham_u8_t *pbuffer=0, buffer[12]={0};
        ham_size_t size=0;

        for (ham_size_t i=0; i<10000; i++) {
            c->insert((ham_offset_t)i, sizeof(buffer), buffer);
        }

        for (ham_size_t i=0; i<10000; i++) {
            BFC_ASSERT_EQUAL(0,
                c->fetch((ham_offset_t)i, &size, &pbuffer));
            BFC_ASSERT_EQUAL((ham_size_t)12, size);
        }

        for (ham_size_t i=0; i<10000; i++) {
             c->remove((ham_offset_t)i);
        }

        for (ham_size_t i=0; i<10000; i++) {
            BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                        c->fetch((ham_offset_t)i, 0, 0));
        }
    }

    void purgeTest(void)
    {
        ExtKeyCache *c=((Database *)m_db)->get_extkey_cache();
        ham_u8_t *pbuffer, buffer[12]={0};
        ham_size_t size;

        for (int i=0; i<20; i++) {
            c->insert((ham_offset_t)i, sizeof(buffer), buffer);
        }

        ham_env_t *env=ham_get_env(m_db);
        Environment *e=(Environment *)env;
        e->set_txn_id(e->get_txn_id()+2000);

        c->purge();

        for (int i=0; i<20; i++) {
            BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                c->fetch((ham_offset_t)i, &size, &pbuffer));
        }
    }
};

BFC_REGISTER_FIXTURE(ExtendedKeyTest);

