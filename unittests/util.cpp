/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * unit tests for mem.h/mem.c
 *
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
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT(ham_new(&m_db)==HAM_SUCCESS);
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT(ham_create_ex(m_db, 0, HAM_IN_MEMORY_DB, 0644, 
                        4096, 0, 0)==HAM_SUCCESS);
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT(ham_close(m_db)==HAM_SUCCESS);
        ham_delete(m_db);
        m_db=0;
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void copyKeyTest(void)
    {
        ham_key_t src, dest;

        src.data=(void *)"hallo welt";
        src.size=::strlen((char *)src.data)+1;
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

        src._ptr=0x12345;
        src._keysize=0;
        src._flags=0;
        src._key[0]=0;

        CPPUNIT_ASSERT(util_copy_key_int2pub(m_db, &src, &dest));
        CPPUNIT_ASSERT(dest.size==0);
        CPPUNIT_ASSERT(dest.data==0);
        CPPUNIT_ASSERT(dest.flags&KEY_BLOB_SIZE_EMPTY);
    }

    void copyKeyInt2PubTinyTest(void)
    {
        int_key_t src;
        ham_key_t dest;

        src._ptr=0x12345;
        src._keysize=1;
        src._flags=0;
        src._key[0]='a';

        CPPUNIT_ASSERT(util_copy_key_int2pub(m_db, &src, &dest));
        CPPUNIT_ASSERT(dest.size==1);
        CPPUNIT_ASSERT(((char *)dest.data)[0]=='a');
        CPPUNIT_ASSERT(dest.flags&KEY_BLOB_SIZE_TINY);
    }

    void copyKeyInt2PubSmallTest(void)
    {
        char buffer[128];
        int_key_t *src=(int_key_t *)buffer;
        ham_key_t dest;

        src->_ptr=0x12345;
        src->_keysize=8;
        src->_flags=0;
        ::strcpy((char *)src->_key, "1234567\0");

        CPPUNIT_ASSERT(util_copy_key_int2pub(m_db, src, &dest));
        CPPUNIT_ASSERT(dest.size==src->_keysize);
        CPPUNIT_ASSERT(!::strcmp((char *)dest.data, (char *)src->_key));
        CPPUNIT_ASSERT(dest.flags&KEY_BLOB_SIZE_SMALL);
    }

    void copyKeyInt2PubFullTest(void)
    {
        char buffer[128];
        int_key_t *src=(int_key_t *)buffer;
        ham_key_t dest;

        src->_ptr=0x12345;
        src->_keysize=16;
        src->_flags=0;
        ::strcpy((char *)src->_key, "123456781234567\0");

        CPPUNIT_ASSERT(util_copy_key_int2pub(m_db, src, &dest));
        CPPUNIT_ASSERT(dest.size==src->_keysize);
        CPPUNIT_ASSERT(!::strcmp((char *)dest.data, (char *)src->_key));
        CPPUNIT_ASSERT(dest.flags&KEY_BLOB_SIZE_SMALL);

        ham_mem_free(m_db, dest.data);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(UtilTest);

