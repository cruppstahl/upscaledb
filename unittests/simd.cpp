/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#ifdef __SSE__

#include "3rdparty/catch/catch.hpp"

#include "2simd/simd.h"

using namespace upscaledb;

template<typename T, int S>
static inline void
test_linear_search_sse()
{
  std::array<T, S> values;
  for (size_t i = 0; i < values.size(); i++)
    values[i] = i + 1;

  REQUIRE((-1 == linear_search_sse<T>(&values[0], 0, values.size(), (T)0)));

  REQUIRE((-1 == linear_search_sse<T>(&values[0], 0, values.size(),
                          (T)values.size() + 1)));

  for (size_t i = 0; i < values.size(); i++)
    REQUIRE(i == linear_search_sse<T>(&values[0], 0, values.size(),
                            (T)(i + 1)));
}

TEST_CASE("Simd/uint16SseTest")
{
  test_linear_search_sse<uint16_t, 16>();
}

TEST_CASE("Simd/uint32SseTest")
{
  test_linear_search_sse<uint32_t, 16>();
}

TEST_CASE("Simd/uint64SseTest")
{
  test_linear_search_sse<uint64_t, 4>();
}

TEST_CASE("Simd/floatSseTest")
{
  test_linear_search_sse<float, 16>();
}

TEST_CASE("Simd/doubleSseTest", "")
{
  test_linear_search_sse<double, 4>();
}

#endif // __SSE__
