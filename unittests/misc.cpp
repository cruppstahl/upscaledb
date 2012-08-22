/**
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
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
#include <string.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/page.h"
#include "../src/util.h"
#include "../src/btree.h"
#include "../src/env.h"
#include "../src/btree_key.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace ham;

class MiscTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    MiscTest()
    :   hamsterDB_fixture("MiscTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(MiscTest, copyKeyTest);
        BFC_REGISTER_TEST(MiscTest, copyExtendedKeyTest);
        BFC_REGISTER_TEST(MiscTest, copyKeyInt2PubEmptyTest);
        BFC_REGISTER_TEST(MiscTest, copyKeyInt2PubTinyTest);
        BFC_REGISTER_TEST(MiscTest, copyKeyInt2PubSmallTest);
        BFC_REGISTER_TEST(MiscTest, copyKeyInt2PubFullTest);
    }

protected:
    ham_db_t *m_db;
    ham_env_t *m_env;

public:
    virtual void setup()
    {
        __super::setup();

        ham_parameter_t p[]={{HAM_PARAM_PAGESIZE, 4096}, {0, 0}};

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, ham_env_new(&m_env));
        BFC_ASSERT_EQUAL(0,
                    ham_env_create_ex(m_env, 0, HAM_IN_MEMORY_DB, 0644, &p[0]));
        BFC_ASSERT_EQUAL(0,
                    ham_env_create_db(m_env, m_db, 1, 0, 0));
    }

    virtual void teardown()
    {
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(m_env, 0));
        ham_delete(m_db);
        ham_env_delete(m_env);
        m_db=0;
        m_env=0;
    }

    void copyKeyTest(void)
    {
        ham_key_t src;
        ham_key_t dest = {0};

        src.data=(void *)"hallo welt";
        src.size=(ham_u16_t)::strlen((char *)src.data)+1;
        src.flags=0;
        src._flags=0;

        BFC_ASSERT_EQUAL(0, ((Database *)m_db)->copy_key(&src, &dest));
        BFC_ASSERT_EQUAL(dest.size, src.size);
        BFC_ASSERT_EQUAL(0, ::strcmp((char *)dest.data, (char *)src.data));

        ((Environment *)m_env)->get_allocator()->free(dest.data);
    }

    void copyExtendedKeyTest(void)
    {
        ham_key_t src;
        ham_key_t dest = {0};

        src.data=(void *)"hallo welt, this is an extended key";
        src.size=(ham_u16_t)::strlen((char *)src.data)+1;
        src.flags=0;
        src._flags=0;

        BFC_ASSERT_EQUAL(0, ((Database *)m_db)->copy_key(&src, &dest));
        BFC_ASSERT_EQUAL(dest.size, src.size);
        BFC_ASSERT_EQUAL(0, ::strcmp((char *)dest.data, (char *)src.data));

        ((Environment *)m_env)->get_allocator()->free(dest.data);
    }

    void copyKeyInt2PubEmptyTest(void)
    {
        btree_key_t src;
        ham_key_t dest;
        memset(&src, 0, sizeof(src));
        memset(&dest, 0, sizeof(dest));

        key_set_ptr(&src, 0x12345);
        key_set_size(&src, 0);
        key_set_flags(&src, 0);

        BFC_ASSERT_EQUAL(0, btree_copy_key_int2pub((Database *)m_db, &src, &dest));
        BFC_ASSERT_EQUAL(0, dest.size);
        BFC_ASSERT_EQUAL((void *)0, dest.data);
    }

    void copyKeyInt2PubTinyTest(void)
    {
        btree_key_t src;
        ham_key_t dest;
        memset(&src, 0, sizeof(src));
        memset(&dest, 0, sizeof(dest));

        key_set_ptr(&src, 0x12345);
        key_set_size(&src, 1);
        key_set_flags(&src, 0);
        src._key[0]='a';

        BFC_ASSERT_EQUAL(0, btree_copy_key_int2pub((Database *)m_db, &src, &dest));
        BFC_ASSERT_EQUAL(1, dest.size);
        BFC_ASSERT_EQUAL('a', ((char *)dest.data)[0]);
        ((Environment *)m_env)->get_allocator()->free(dest.data);
    }

    void copyKeyInt2PubSmallTest(void)
    {
        char buffer[128];
        btree_key_t *src=(btree_key_t *)buffer;
        ham_key_t dest;
        memset(&dest, 0, sizeof(dest));

        key_set_ptr(src, 0x12345);
        key_set_size(src, 8);
        key_set_flags(src, 0);
        ::memcpy((char *)src->_key, "1234567\0", 8);

        BFC_ASSERT_EQUAL(0, btree_copy_key_int2pub((Database *)m_db, src, &dest));
        BFC_ASSERT_EQUAL(dest.size, key_get_size(src));
        BFC_ASSERT_EQUAL(0, ::strcmp((char *)dest.data, (char *)src->_key));
        ((Environment *)m_env)->get_allocator()->free(dest.data);
    }

    void copyKeyInt2PubFullTest(void)
    {
        char buffer[128];
        btree_key_t *src=(btree_key_t *)buffer;
        ham_key_t dest;
        memset(&dest, 0, sizeof(dest));

        key_set_ptr(src, 0x12345);
        key_set_size(src, 16);
        key_set_flags(src, 0);
        ::strcpy((char *)&buffer[11] /*src->_key*/, "123456781234567\0");

        BFC_ASSERT_EQUAL(0, btree_copy_key_int2pub((Database *)m_db, src, &dest));
        BFC_ASSERT_EQUAL(dest.size, key_get_size(src));
        BFC_ASSERT_EQUAL(0, ::strcmp((char *)dest.data, (char *)src->_key));

        ((Environment *)m_env)->get_allocator()->free(dest.data);
    }

};

BFC_REGISTER_FIXTURE(MiscTest);

