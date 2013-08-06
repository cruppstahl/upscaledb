/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "3rdparty/catch/catch.hpp"

#include "globals.h"

#include "../src/endianswap.h"

TEST_CASE("EndianTest/isLittleEndian",
           "Tests if the target is little endian")
{
#ifndef HAM_BIG_ENDIAN
  REQUIRE(HAM_LITTLE_ENDIAN);
#else
  REQUIRE(HAM_BIG_ENDIAN);
#endif
}

TEST_CASE("EndianTest/byteswap16",
           "Tests the little endian/big endian byte swapping macros")
{
  REQUIRE(0x3412 == _ham_byteswap16(0x1234));
  REQUIRE(0xafbc == _ham_byteswap16(0xbcaf));
  REQUIRE(0x0000 == _ham_byteswap16(0x0000));
  REQUIRE(0xffff == _ham_byteswap16(0xffff));
}

TEST_CASE("EndianTest/byteswap32",
           "Tests the little endian/big endian byte swapping macros")
{
  REQUIRE((unsigned int)0x78563412 == _ham_byteswap32(0x12345678));
  REQUIRE((unsigned int)0xafbc1324 == _ham_byteswap32(0x2413bcaf));
  REQUIRE((unsigned int)0x00000000 == _ham_byteswap32(0x00000000));
  REQUIRE((unsigned int)0xffffffff == _ham_byteswap32(0xffffffff));
}

TEST_CASE("EndianTest/byteswap64",
           "Tests the little endian/big endian byte swapping macros")
{
  REQUIRE((unsigned long long)0x3210cba987654321ull ==
      _ham_byteswap64(0x21436587a9cb1032ull));
  REQUIRE((unsigned long long)0xafbc132423abcf09ull ==
      _ham_byteswap64(0x09cfab232413bcafull));
  REQUIRE((unsigned long long)0x0000000000000000ull ==
      _ham_byteswap64(0x0000000000000000ull));
  REQUIRE((unsigned long long)0xffffffffffffffffull ==
      _ham_byteswap64(0xffffffffffffffffull));
}

TEST_CASE("EndianTest/byteswapTwice16",
           "Tests the little endian/big endian byte swapping macros")
{
  unsigned short swapped, orig, d[] = {0x1234, 0xafbc, 0, 0xffff};
  for (int i = 0; i < 4; i++) {
    orig = d[i];
    swapped = _ham_byteswap16(orig);
    REQUIRE(orig == (unsigned short)_ham_byteswap16(swapped));
  }
}

TEST_CASE("EndianTest/byteswapTwice32",
           "Tests the little endian/big endian byte swapping macros")
{
  unsigned int swapped, orig, d[] = {0x12345678, 0xafbc2389, 0, 0xffffffff};
  for (int i = 0; i < 4; i++) {
    orig = d[i];
    swapped = _ham_byteswap32(orig);
    REQUIRE(orig == (unsigned int)_ham_byteswap32(swapped));
  }
}

TEST_CASE("EndianTest/byteswapTwice64",
           "Tests the little endian/big endian byte swapping macros")
{
  unsigned long long swapped, orig, d[] = {0x12345678abcd0123ull,
      0xafbc238919475868ull, 0ull, 0xffffffffffffffffull};
  for (int i = 0; i < 4; i++) {
    orig = d[i];
    swapped = _ham_byteswap64(orig);
    REQUIRE(orig == (unsigned long long)_ham_byteswap64(swapped));
  }
}

