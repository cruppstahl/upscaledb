/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 */

#include <stdexcept>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/page.h"
#include "../src/util.h"
#include "memtracker.h"

class UtilTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(UtilTest);
    CPPUNIT_TEST      (copyKeyTest);
    CPPUNIT_TEST      (copyKeyInt2PubEmptyTest);
    CPPUNIT_TEST      (copyKeyInt2PubTinyTest);
    CPPUNIT_TEST      (copyKeyInt2PubSmallTest);
    CPPUNIT_TEST      (copyKeyInt2PubFullTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        ham_parameter_t p[]={{HAM_PARAM_PAGESIZE, 4096}, {0, 0}};

        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT(ham_new(&m_db)==HAM_SUCCESS);
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT(ham_create_ex(m_db, 0, HAM_IN_MEMORY_DB, 0644, 
                        &p[0])==HAM_SUCCESS);
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        m_db=0;
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void copyKeyTest(void)
    {
        ham_key_t src, dest;

        src.data=(void *)"hallo welt";
        src.size=(ham_u16_t)::strlen((char *)src.data)+1;
        src.flags=0;
        src._flags=0;

        CPPUNIT_ASSERT(util_copy_key(m_db, &src, &dest));
        CPPUNIT_ASSERT(dest.size==src.size);
        CPPUNIT_ASSERT(!::strcmp((char *)dest.data, (char *)src.data));

        ham_mem_free(m_db, dest.data);
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

        CPPUNIT_ASSERT(util_copy_key_int2pub(m_db, &src, &dest));
        CPPUNIT_ASSERT(dest.size==0);
        CPPUNIT_ASSERT(dest.data==0);
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

        CPPUNIT_ASSERT(util_copy_key_int2pub(m_db, &src, &dest));
        CPPUNIT_ASSERT(1==dest.size);
        CPPUNIT_ASSERT('a'==((char *)dest.data)[0]);
        ham_mem_free(m_db, dest.data);
    }

    void copyKeyInt2PubSmallTest(void)
    {
        char buffer[128];
        int_key_t *src=(int_key_t *)buffer;
        ham_key_t dest;

        key_set_ptr(src, 0x12345);
        key_set_size(src, 8);
        key_set_flags(src, 0);
        ::strcpy((char *)src->_key, "1234567\0");

        CPPUNIT_ASSERT(util_copy_key_int2pub(m_db, src, &dest));
        CPPUNIT_ASSERT(dest.size==(ham_size_t)key_get_size(src));
        CPPUNIT_ASSERT(!::strcmp((char *)dest.data, (char *)src->_key));
        ham_mem_free(m_db, dest.data);
    }

    void copyKeyInt2PubFullTest(void)
    {
        char buffer[128];
        int_key_t *src=(int_key_t *)buffer;
        ham_key_t dest;

        key_set_ptr(src, 0x12345);
        key_set_size(src, 16);
        key_set_flags(src, 0);
        ::strcpy((char *)src->_key, "123456781234567\0");

        CPPUNIT_ASSERT(util_copy_key_int2pub(m_db, src, &dest));
        CPPUNIT_ASSERT(dest.size==(ham_size_t)key_get_size(src));
        CPPUNIT_ASSERT(!::strcmp((char *)dest.data, (char *)src->_key));

        ham_mem_free(m_db, dest.data);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(UtilTest);

