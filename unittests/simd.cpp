/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#ifdef __SSE__

#include "3rdparty/catch/catch.hpp"

#include "2simd/simd.h"
#include <array>

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
    REQUIRE((int)i == linear_search_sse<T>(&values[0], 0, values.size(),
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
