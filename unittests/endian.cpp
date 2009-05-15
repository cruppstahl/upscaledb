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

#include "../src/endian.h"
#include "bfc-testsuite.hpp"

using namespace bfc;

class EndianTest : public fixture
{
public:
    EndianTest()
    :   fixture("EndianTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(EndianTest, byteswap16);
        BFC_REGISTER_TEST(EndianTest, byteswap32);
        BFC_REGISTER_TEST(EndianTest, byteswap64);
        BFC_REGISTER_TEST(EndianTest, byteswapTwice16);
        BFC_REGISTER_TEST(EndianTest, byteswapTwice32);
        BFC_REGISTER_TEST(EndianTest, byteswapTwice64);
    }

public:
    void byteswap16() {
        BFC_ASSERT_EQUAL(0x3412, 
                             _ham_byteswap16(0x1234));
        BFC_ASSERT_EQUAL(0xafbc, 
                             _ham_byteswap16(0xbcaf));
        BFC_ASSERT_EQUAL(0x0000, 
                             _ham_byteswap16(0x0000));
        BFC_ASSERT_EQUAL(0xffff, 
                             _ham_byteswap16(0xffff));
    }

    void byteswap32() {
        BFC_ASSERT_EQUAL((unsigned int)0x78563412, 
                             _ham_byteswap32(0x12345678));
        BFC_ASSERT_EQUAL((unsigned int)0xafbc1324, 
                             _ham_byteswap32(0x2413bcaf));
        BFC_ASSERT_EQUAL((unsigned int)0x00000000, 
                             _ham_byteswap32(0x00000000));
        BFC_ASSERT_EQUAL((unsigned int)0xffffffff, 
                             _ham_byteswap32(0xffffffff));
    }

    void byteswap64() {
        BFC_ASSERT_EQUAL((unsigned long long)0x3210cba987654321ull, 
                             _ham_byteswap64(0x21436587a9cb1032ull));
        BFC_ASSERT_EQUAL((unsigned long long)0xafbc132423abcf09ull, 
                             _ham_byteswap64(0x09cfab232413bcafull));
        BFC_ASSERT_EQUAL((unsigned long long)0x0000000000000000ull, 
                             _ham_byteswap64(0x0000000000000000ull));
        BFC_ASSERT_EQUAL((unsigned long long)0xffffffffffffffffull, 
                             _ham_byteswap64(0xffffffffffffffffull));
    }

    void byteswapTwice16() {
        unsigned short swapped, orig, d[]={0x1234, 0xafbc, 0, 0xffff};
        for (int i=0; i<4; i++) {
            orig=d[i];
            swapped=_ham_byteswap16(orig);
            BFC_ASSERT_EQUAL(orig, 
                        (unsigned short)_ham_byteswap16(swapped));
        }
    }

    void byteswapTwice32() {
        unsigned int swapped, orig, d[]={0x12345678, 0xafbc2389, 0, 0xffffffff};
        for (int i=0; i<4; i++) {
            orig=d[i];
            swapped=_ham_byteswap32(orig);
            BFC_ASSERT_EQUAL(orig, 
                        (unsigned int)_ham_byteswap32(swapped));
        }
    }

    void byteswapTwice64() {
        unsigned long long swapped, orig, d[]={0x12345678abcd0123ull, 
                0xafbc238919475868ull, 0ull, 0xffffffffffffffffull};
        for (int i=0; i<4; i++) {
            orig=d[i];
            swapped=_ham_byteswap64(orig);
            BFC_ASSERT_EQUAL(orig, 
                        (unsigned long long)_ham_byteswap64(swapped));
        }
    }

};

BFC_REGISTER_FIXTURE(EndianTest);
