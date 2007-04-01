/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * unit tests for endian.h
 *
 */

#include <cppunit/extensions/HelperMacros.h>
#include "../src/endian.h"

class EndianTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EndianTest);
    CPPUNIT_TEST      (byteswap16);
    CPPUNIT_TEST      (byteswap32);
    CPPUNIT_TEST      (byteswap64);
    CPPUNIT_TEST      (byteswapTwice16);
    CPPUNIT_TEST      (byteswapTwice32);
    CPPUNIT_TEST      (byteswapTwice64);
    CPPUNIT_TEST_SUITE_END();

public:
    void setUp()    { }
    void tearDown() { }

    void byteswap16() {
        CPPUNIT_ASSERT_EQUAL(0x3412, 
                             _ham_byteswap16(0x1234));
        CPPUNIT_ASSERT_EQUAL(0xafbc, 
                             _ham_byteswap16(0xbcaf));
        CPPUNIT_ASSERT_EQUAL(0x0000, 
                             _ham_byteswap16(0x0000));
        CPPUNIT_ASSERT_EQUAL(0xffff, 
                             _ham_byteswap16(0xffff));
    }

    void byteswap32() {
        CPPUNIT_ASSERT_EQUAL((unsigned int)0x78563412, 
                             _ham_byteswap32(0x12345678));
        CPPUNIT_ASSERT_EQUAL((unsigned int)0xafbc1324, 
                             _ham_byteswap32(0x2413bcaf));
        CPPUNIT_ASSERT_EQUAL((unsigned int)0x00000000, 
                             _ham_byteswap32(0x00000000));
        CPPUNIT_ASSERT_EQUAL((unsigned int)0xffffffff, 
                             _ham_byteswap32(0xffffffff));
    }

    void byteswap64() {
        CPPUNIT_ASSERT_EQUAL((unsigned long long)0x3210cba987654321, 
                             _ham_byteswap64(0x21436587a9cb1032));
        CPPUNIT_ASSERT_EQUAL((unsigned long long)0xafbc132423abcf09, 
                             _ham_byteswap64(0x09cfab232413bcaf));
        CPPUNIT_ASSERT_EQUAL((unsigned long long)0x0000000000000000, 
                             _ham_byteswap64(0x0000000000000000));
        CPPUNIT_ASSERT_EQUAL((unsigned long long)0xffffffffffffffff, 
                             _ham_byteswap64(0xffffffffffffffff));
    }

    void byteswapTwice16() {
        unsigned short swapped, orig, d[]={0x1234, 0xafbc, 0, 0xffff};
        for (int i=0; i<4; i++) {
            orig=d[i];
            swapped=_ham_byteswap16(orig);
            CPPUNIT_ASSERT_EQUAL(orig, 
                        (unsigned short)_ham_byteswap16(swapped));
        }
    }

    void byteswapTwice32() {
        unsigned int swapped, orig, d[]={0x12345678, 0xafbc2389, 0, 0xffffffff};
        for (int i=0; i<4; i++) {
            orig=d[i];
            swapped=_ham_byteswap32(orig);
            CPPUNIT_ASSERT_EQUAL(orig, 
                        (unsigned int)_ham_byteswap32(swapped));
        }
    }

    void byteswapTwice64() {
        unsigned long long swapped, orig, d[]={0x12345678abcd0123, 
                0xafbc238919475868, 0, 0xffffffffffffffff};
        for (int i=0; i<4; i++) {
            orig=d[i];
            swapped=_ham_byteswap64(orig);
            CPPUNIT_ASSERT_EQUAL(orig, 
                        (unsigned long long)_ham_byteswap64(swapped));
        }
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(EndianTest);
