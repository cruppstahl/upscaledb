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
#include "../src/mem.h"
#include "memtracker.h"

class MemoryTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(MemoryTest);
    CPPUNIT_TEST      (simpleTest);
    CPPUNIT_TEST      (trackingTest);
    CPPUNIT_TEST      (trackingTest2);
    CPPUNIT_TEST      (freeNullTest);
    CPPUNIT_TEST_SUITE_END();

public:
    void setUp()    { }
    void tearDown() { }

    void simpleTest() {
        void *p;
        mem_allocator_t *alloc=ham_default_allocator_new();
        p=alloc->alloc(alloc, __FILE__, __LINE__, 128);
        CPPUNIT_ASSERT(p);
        alloc->free(alloc, __FILE__, __LINE__, p);
        alloc->close(alloc);
    }

    /*
     * TODO ham_mem_free_null is missing
     */

    void trackingTest() {
        void *p;
        memtracker_t *alloc=memtracker_new();
        p=alloc->alloc((mem_allocator_t *)alloc, __FILE__, __LINE__, 128);
        CPPUNIT_ASSERT(p);
        alloc->free((mem_allocator_t *)alloc, __FILE__, __LINE__, p);
        CPPUNIT_ASSERT(!memtracker_get_leaks(alloc));
        alloc->close((mem_allocator_t *)alloc);
    }

    void trackingTest2() {
        void *p[3];
        memtracker_t *alloc=memtracker_new();
        p[0]=alloc->alloc((mem_allocator_t *)alloc, __FILE__, __LINE__, 10);
        CPPUNIT_ASSERT(p[0]);
        p[1]=alloc->alloc((mem_allocator_t *)alloc, __FILE__, __LINE__, 12);
        CPPUNIT_ASSERT(p[1]);
        p[2]=alloc->alloc((mem_allocator_t *)alloc, __FILE__, __LINE__, 14);
        CPPUNIT_ASSERT(p[2]);
        alloc->free((mem_allocator_t *)alloc, __FILE__, __LINE__, p[0]);
        CPPUNIT_ASSERT(memtracker_get_leaks(alloc)==26);
        alloc->free((mem_allocator_t *)alloc, __FILE__, __LINE__, p[1]);
        CPPUNIT_ASSERT(memtracker_get_leaks(alloc)==14);
        alloc->free((mem_allocator_t *)alloc, __FILE__, __LINE__, p[2]);
        CPPUNIT_ASSERT(memtracker_get_leaks(alloc)==0);
        alloc->close((mem_allocator_t *)alloc);
    }

    void freeNullTest() {
        void *p=0;
        memtracker_t *alloc=memtracker_new();
        try {
            alloc->free((mem_allocator_t *)alloc, __FILE__, __LINE__, p);
        } 
        catch (std::logic_error e) {
            CPPUNIT_ASSERT(memtracker_get_leaks(alloc)==0);
            alloc->close((mem_allocator_t *)alloc);
            return;
        }

        CPPUNIT_FAIL("should not be here");
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(MemoryTest);
