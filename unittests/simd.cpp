/**
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifdef HAM_ENABLE_SIMD

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "2simd/simd.h"

using namespace hamsterdb;

template<typename T>
void
test_linear_search_sse()
{
#undef MAX
#define MAX 16
  T arr[MAX];
  for (int i = 0; i < MAX; i++)
    arr[i] = i + 1;

  REQUIRE(-1 == linear_search_sse<T>(&arr[0], 0, MAX, (T)0));

  REQUIRE(-1 == linear_search_sse<T>(&arr[0], 0, MAX, (T)MAX + 1));

  for (int i = 0; i < MAX; i++)
    REQUIRE(i == linear_search_sse<T>(&arr[0], 0, MAX, (T)(i + 1)));
}

TEST_CASE("Simd/uint16SseTest", "")
{
  test_linear_search_sse<uint16_t>();
}

TEST_CASE("Simd/uint32SseTest", "")
{
  test_linear_search_sse<uint32_t>();
}

TEST_CASE("Simd/uint64SseTest", "")
{
#undef MAX
#define MAX 4
  uint64_t arr[MAX];
  for (int i = 0; i < MAX; i++)
    arr[i] = i + 1;

  REQUIRE(-1 == linear_search_sse<uint64_t>(&arr[0], 0, MAX, 0));

  REQUIRE(-1 == linear_search_sse<uint64_t>(&arr[0], 0, MAX, MAX + 1));

  for (int i = 0; i < MAX; i++)
    REQUIRE(i == linear_search_sse<uint64_t>(&arr[0], 0, MAX, (i + 1)));
}

TEST_CASE("Simd/floatSseTest", "")
{
  test_linear_search_sse<float>();
}

TEST_CASE("Simd/doubleSseTest", "")
{
#undef MAX
#define MAX 4
  double arr[MAX];
  for (int i = 0; i < MAX; i++)
    arr[i] = i + 1;

  REQUIRE(-1 == linear_search_sse<double>(&arr[0], 0, MAX, 0));

  REQUIRE(-1 == linear_search_sse<double>(&arr[0], 0, MAX, MAX + 1));

  for (int i = 0; i < MAX; i++)
    REQUIRE(i == linear_search_sse<double>(&arr[0], 0, MAX, (i + 1)));
}

#endif // HAM_ENABLE_SIMD
