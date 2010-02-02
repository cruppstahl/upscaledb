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
#include <string.h> // [i_a] strlen, memcmp, etc.
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/page.h"
#include "../src/util.h"
#include "../src/env.h"
#include "../src/keys.h"
#include "memtracker.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;


class UtilTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    UtilTest()
    :   hamsterDB_fixture("UtilTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(UtilTest, copyKeyTest);
        BFC_REGISTER_TEST(UtilTest, copyExtendedKeyTest);
        BFC_REGISTER_TEST(UtilTest, copyKeyInt2PubEmptyTest);
        BFC_REGISTER_TEST(UtilTest, copyKeyInt2PubTinyTest);
        BFC_REGISTER_TEST(UtilTest, copyKeyInt2PubSmallTest);
        BFC_REGISTER_TEST(UtilTest, copyKeyInt2PubFullTest);
    }

protected:
    ham_db_t *m_db;
    ham_env_t *m_env;
    memtracker_t *m_alloc;

public:
    virtual void setup() 
    { 
        __super::setup();

        ham_parameter_t p[]={{HAM_PARAM_PAGESIZE, 4096}, {0, 0}};

        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT(ham_new(&m_db)==HAM_SUCCESS);
        //db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT(ham_create_ex(m_db, 0, HAM_IN_MEMORY_DB, 0644, 
                        &p[0])==HAM_SUCCESS);

        m_env=db_get_env(m_db);
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        m_db=0;
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void copyKeyTest(void)
    {
        ham_key_t src;
        ham_key_t dest = {0};

        src.data=(void *)"hallo welt";
        src.size=(ham_u16_t)::strlen((char *)src.data)+1;
        src.flags=0;
        src._flags=0;

        BFC_ASSERT(util_copy_key(m_db, &src, &dest));
        BFC_ASSERT(dest.size==src.size);
        BFC_ASSERT(!::strcmp((char *)dest.data, (char *)src.data));

        allocator_free(env_get_allocator(m_env), dest.data);
    }

    void copyExtendedKeyTest(void)
    {
        ham_key_t src;
        ham_key_t dest = {0};

        src.data=(void *)"hallo welt, this is an extended key";
        src.size=(ham_u16_t)::strlen((char *)src.data)+1;
        src.flags=0;
        src._flags=0;

        BFC_ASSERT(util_copy_key(m_db, &src, &dest));
        BFC_ASSERT(dest.size==src.size);
        BFC_ASSERT(!::strcmp((char *)dest.data, (char *)src.data));

        allocator_free(env_get_allocator(m_env), dest.data);
    }

    void copyKeyInt2PubEmptyTest(void)
    {
        int_key_t src;
        ham_key_t dest;
        memset(&src, 0, sizeof(src));
        memset(&dest, 0, sizeof(dest));

        key_set_ptr(&src, 0x12345);
        key_set_size(&src, 0);
        key_set_flags(&src, 0);
        src._key[0]=0;

        BFC_ASSERT(util_copy_key_int2pub(m_db, &src, &dest));
        BFC_ASSERT(dest.size==0);
        BFC_ASSERT(dest.data==0);
    }

    void copyKeyInt2PubTinyTest(void)
    {
        int_key_t src;
        ham_key_t dest;
        memset(&src, 0, sizeof(src));
        memset(&dest, 0, sizeof(dest));

        key_set_ptr(&src, 0x12345);
        key_set_size(&src, 1);
        key_set_flags(&src, 0);
        src._key[0]='a';

        BFC_ASSERT(util_copy_key_int2pub(m_db, &src, &dest));
        BFC_ASSERT(1==dest.size);
        BFC_ASSERT('a'==((char *)dest.data)[0]);
        allocator_free(env_get_allocator(m_env), dest.data);
    }

    void copyKeyInt2PubSmallTest(void)
    {
        char buffer[128];
        int_key_t *src=(int_key_t *)buffer;
        ham_key_t dest;
        memset(&dest, 0, sizeof(dest));

        key_set_ptr(src, 0x12345);
        key_set_size(src, 8);
        key_set_flags(src, 0);
        ::strcpy((char *)src->_key, "1234567\0");

        BFC_ASSERT(util_copy_key_int2pub(m_db, src, &dest));
        BFC_ASSERT(dest.size==(ham_size_t)key_get_size(src));
        BFC_ASSERT(!::strcmp((char *)dest.data, (char *)src->_key));
        allocator_free(env_get_allocator(m_env), dest.data);
    }

    void copyKeyInt2PubFullTest(void)
    {
        char buffer[128];
        int_key_t *src=(int_key_t *)buffer;
        ham_key_t dest;
        memset(&dest, 0, sizeof(dest));

        key_set_ptr(src, 0x12345);
        key_set_size(src, 16);
        key_set_flags(src, 0);
        ::strcpy((char *)src->_key, "123456781234567\0");

        BFC_ASSERT(util_copy_key_int2pub(m_db, src, &dest));
        BFC_ASSERT(dest.size==(ham_size_t)key_get_size(src));
        BFC_ASSERT(!::strcmp((char *)dest.data, (char *)src->_key));

        allocator_free(env_get_allocator(m_env), dest.data);
    }

};

BFC_REGISTER_FIXTURE(UtilTest);

