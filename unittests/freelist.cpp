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
#include "../src/freelist.h"
#include "memtracker.h"

class FreelistTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(FreelistTest);
    CPPUNIT_TEST      (createShutdownTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (addAllocTest);
    CPPUNIT_TEST      (negativeAllocTest);
    CPPUNIT_TEST      (addAllocOverflowTest);
    CPPUNIT_TEST      (addAllocSplitTest);
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
        CPPUNIT_ASSERT(ham_create(m_db, ".test", 0, 0644)==HAM_SUCCESS);
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT(ham_close(m_db)==HAM_SUCCESS);
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void createShutdownTest(void)
    {
        CPPUNIT_ASSERT(freel_create(m_db)==HAM_SUCCESS);
        CPPUNIT_ASSERT(freel_shutdown(m_db)==HAM_SUCCESS);
    }

    void structureTest(void)
    {
        ham_page_t *page;
        freel_payload_t *fp;
        db_header_t *hdr;

        CPPUNIT_ASSERT(freel_create(m_db)==HAM_SUCCESS);
        page=db_get_header_page(m_db);
        hdr=(db_header_t *)page_get_payload(page);
        fp=&hdr->_freelist;

        freel_payload_set_count(fp, 13);
        CPPUNIT_ASSERT(freel_payload_get_count(fp)==13);
        freel_payload_set_maxsize(fp, 20);
        CPPUNIT_ASSERT(freel_payload_get_maxsize(fp)==20);
        freel_payload_set_overflow(fp, 0x123ull);
        CPPUNIT_ASSERT(freel_payload_get_overflow(fp)==0x123ull);
        freel_payload_set_overflow(fp, 0ull);
        CPPUNIT_ASSERT(freel_payload_get_overflow(fp)==0ull);

        CPPUNIT_ASSERT(freel_shutdown(m_db)==HAM_SUCCESS);
    }

    void addAllocTest(void)
    {
        CPPUNIT_ASSERT(freel_add_area(m_db, 0x100ull, 100)==HAM_SUCCESS);
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 100, FREEL_DONT_ALIGN)==0x100ull);
        CPPUNIT_ASSERT(freel_add_area(m_db, 0x100ull, 100)==HAM_SUCCESS);
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 50, FREEL_DONT_ALIGN)==0x100ull);
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 20, FREEL_DONT_ALIGN)==0x150ull);
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 30, FREEL_DONT_ALIGN)==0x170ull);
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 30, FREEL_DONT_ALIGN)==0);
    }

    void negativeAllocTest(void)
    {
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 100, 0)==0);
        CPPUNIT_ASSERT(freel_add_area(m_db, 0x100ull, 100)==HAM_SUCCESS);
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 101, FREEL_DONT_ALIGN)==0);
    }

    void addAllocOverflowTest(void)
    {
        ham_page_t *page;
        freel_payload_t *fp;
        db_header_t *hdr;

        page=db_get_header_page(m_db);
        hdr=(db_header_t *)page_get_payload(page);
        fp=&hdr->_freelist;

        for (int i=0; i<freel_payload_get_maxsize(fp)+10; i++) {
            CPPUNIT_ASSERT(freel_add_area(m_db, 0x100ull, i*100)==HAM_SUCCESS);
        }

        for (int i=0; i<freel_payload_get_maxsize(fp)+10; i++) {
            CPPUNIT_ASSERT(freel_alloc_area(m_db, 0x100ull, 
                        FREEL_DONT_ALIGN)==i*100);
        }

        CPPUNIT_ASSERT(freel_alloc_area(m_db, 101, FREEL_DONT_ALIGN)==0);
    }
    
    void addAllocOverflowReopenTest(void)
    {
        ham_page_t *page;
        freel_payload_t *fp;
        db_header_t *hdr;

        page=db_get_header_page(m_db);
        hdr=(db_header_t *)page_get_payload(page);
        fp=&hdr->_freelist;

        for (int i=0; i<freel_payload_get_maxsize(fp)+10; i++) {
            CPPUNIT_ASSERT(freel_add_area(m_db, 0x100ull, i*100)==HAM_SUCCESS);
        }

        CPPUNIT_ASSERT(ham_close(m_db)==HAM_SUCCESS);
        CPPUNIT_ASSERT(ham_open(m_db, ".test", 0)==HAM_SUCCESS);

        for (int i=0; i<freel_payload_get_maxsize(fp)+10; i++) {
            CPPUNIT_ASSERT(freel_alloc_area(m_db, 0x100ull, 
                        FREEL_DONT_ALIGN)==i*100);
        }

        CPPUNIT_ASSERT(freel_alloc_area(m_db, 101, FREEL_DONT_ALIGN)==0);
    }

    void addAllocSplitTest(void)
    {
    }

    /* TODO auch mit alignment testen! */

};

CPPUNIT_TEST_SUITE_REGISTRATION(FreelistTest);

