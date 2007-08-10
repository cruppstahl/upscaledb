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
#include "../src/btree.h"
#include "../src/keys.h"
#include "memtracker.h"

class KeyTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(KeyTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (extendedRidTest);
    CPPUNIT_TEST      (endianTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, 0, HAM_IN_MEMORY_DB, 0));
    }
    
    void tearDown() 
    { 
        ham_close(m_db);
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void structureTest(void)
    {
        ham_page_t *page=page_new(m_db);
        CPPUNIT_ASSERT(page!=0);
        CPPUNIT_ASSERT_EQUAL(0, page_alloc(page, db_get_pagesize(m_db)));
        btree_node_t *node=ham_page_get_btree_node(page);
        ::memset(node, 0, db_get_usable_pagesize(m_db));

        int_key_t *key=btree_node_get_key(m_db, node, 0);
        CPPUNIT_ASSERT_EQUAL((ham_offset_t)0, key_get_ptr(key));
        CPPUNIT_ASSERT_EQUAL((ham_u8_t)0, key_get_flags(key));
        CPPUNIT_ASSERT_EQUAL((ham_u8_t)'\0', *key_get_key(key));

        key_set_ptr(key, (ham_offset_t)0x12345);
        CPPUNIT_ASSERT_EQUAL((ham_offset_t)0x12345, key_get_ptr(key));

        key_set_flags(key, (ham_u8_t)0x13);
        CPPUNIT_ASSERT_EQUAL((ham_u8_t)0x13, key_get_flags(key));

        ::strcpy((char *)key_get_key(key), "abc");
        CPPUNIT_ASSERT_EQUAL(0, ::strcmp((char *)key_get_key(key), "abc"));

        CPPUNIT_ASSERT_EQUAL(0, page_free(page));
        page_delete(page);
    }

    void extendedRidTest(void)
    {
        ham_page_t *page=page_new(m_db);
        CPPUNIT_ASSERT(page!=0);
        CPPUNIT_ASSERT_EQUAL(0, page_alloc(page, db_get_pagesize(m_db)));
        btree_node_t *node=ham_page_get_btree_node(page);
        ::memset(node, 0, db_get_usable_pagesize(m_db));

        ham_offset_t blobid;

        int_key_t *key=btree_node_get_key(m_db, node, 0);
        blobid=key_get_extended_rid(m_db, key);
        CPPUNIT_ASSERT_EQUAL((ham_offset_t)0, blobid);

        key_set_extended_rid(m_db, key, (ham_offset_t)0xbaadbeef);
        blobid=key_get_extended_rid(m_db, key);
        CPPUNIT_ASSERT_EQUAL((ham_offset_t)0xbaadbeef, blobid);

        CPPUNIT_ASSERT_EQUAL(0, page_free(page));
        page_delete(page);
    }
    
    void endianTest(void)
    {
        ham_u8_t buffer[64]={
                0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01,
                0x00, 0x00, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        };

        int_key_t *key=(int_key_t *)&buffer[0];

        CPPUNIT_ASSERT_EQUAL((ham_offset_t)0x0123456789abcdefull, 
                key_get_ptr(key));
        CPPUNIT_ASSERT_EQUAL((ham_u8_t)0xf0, key_get_flags(key));
        CPPUNIT_ASSERT_EQUAL((ham_offset_t)0xfedcba9876543210ull, 
                key_get_extended_rid(m_db, key));
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(KeyTest);

